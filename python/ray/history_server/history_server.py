"""HistoryServer monitoring loop daemon."""

import argparse
import ray
import sys
import json
import asyncio
import logging
from ray._private.ray_logging import setup_component_logger
from ray._private.gcs_pubsub import _AioSubscriber, GcsAioActorSubscriber, GcsAioTaskEventsSubscriber
from ray.core.generated import (
    gcs_service_pb2,
    gcs_service_pb2_grpc,
    node_manager_pb2,
    node_manager_pb2_grpc,
    pubsub_pb2,
)
from typing import Tuple, List
import ray._private.ray_constants as ray_constants
from ray.dashboard.datacenter import DataOrganizer, DataSource
import grpc
from storage import (
    append_job_event,
    append_node_event,
    append_actor_events,
    create_history_server_storage,
)
from ray._private.gcs_utils import GcsAioClient, GcsChannel
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.actor import actor_consts

logger = logging.getLogger(__name__)

def actor_table_data_to_dict(message):
    orig_message = dashboard_utils.message_to_dict(
        message,
        {
            "actorId",
            "parentId",
            "jobId",
            "workerId",
            "rayletId",
            "callerId",
            "taskId",
            "parentTaskId",
            "sourceActorId",
        },
        always_print_fields_with_no_presence=True,
    )
    # The complete schema for actor table is here:
    #     src/ray/protobuf/gcs.proto
    # It is super big and for dashboard, we don't need that much information.
    # Only preserve the necessary ones here for memory usage.
    fields = {
        "actorId",
        "jobId",
        "pid",
        "address",
        "state",
        "name",
        "numRestarts",
        "timestamp",
        "className",
        "startTime",
        "endTime",
        "reprName",
    }
    light_message = {k: v for (k, v) in orig_message.items() if k in fields}
    light_message["actorClass"] = orig_message["className"]
    exit_detail = "-"
    if "deathCause" in orig_message:
        context = orig_message["deathCause"]
        if "actorDiedErrorContext" in context:
            exit_detail = context["actorDiedErrorContext"]["errorMessage"]  # noqa
        elif "runtimeEnvFailedContext" in context:
            exit_detail = context["runtimeEnvFailedContext"]["errorMessage"]  # noqa
        elif "actorUnschedulableContext" in context:
            exit_detail = context["actorUnschedulableContext"]["errorMessage"]  # noqa
        elif "creationTaskFailureContext" in context:
            exit_detail = context["creationTaskFailureContext"][
                "formattedExceptionString"
            ]  # noqa
    light_message["exitDetail"] = exit_detail
    light_message["startTime"] = int(light_message["startTime"])
    light_message["endTime"] = int(light_message["endTime"])
    light_message["requiredResources"] = dict(message.required_resources)

    return light_message



def gcs_node_info_to_dict(message):
    return dashboard_utils.message_to_dict(
        message, {"nodeId"}, always_print_fields_with_no_presence=True
    )

class GcsAioJobSubmissionSubscriber(_AioSubscriber):
    def __init__(
        self,
        worker_id: bytes = None,
        address: str = None,
        channel: grpc.Channel = None,
    ):
        super().__init__(
            pubsub_pb2.RAY_JOB_SUBMISSION_STATUS_CHANNEL, worker_id, address, channel
        )

    @property
    def queue_size(self):
        return len(self._queue)

    async def poll(self, timeout=None, batch_size=500) -> List[Tuple[bytes, str]]:
        """Polls for new actor message.

        Returns:
            A tuple of binary actor ID and actor table data.
        """
        await self._poll(timeout=timeout)
        # return self._pop_actors(self._queue, batch_size=batch_size)
        msgs = []
        while len(self._queue) > 0:
            msg = self._queue.popleft()
            # logger.info(f"GcsAioJobSubmissionSubscriber poll msg: {msg}")
            msgs.append(msg)
        # logger.info(f"GcsAioJobSubmissionSubscriber poll msgs: {msgs}")
        return msgs

def add_history_server_job_fields(submission_id, job_change_message):
    logger.info("add_history_server_job_fields")
    log_dir = os.environ.get("BYTED_RAY_REDIRECT_LOG", "/tmp/ray/session_latest/logs")
    node_id = job_change_message["driver_node_id"]
    node = DataSource.nodes.get(node_id)
    logger.info(f"node: {node}")
    if node:
        # Add podname/containername/logname/psm for generating logagent links.
        node_name = node["nodeName"]
        job_change_message["nodeName"] = node_name
        container_name = ""
        if "-head-" in node_name:
            container_name = "ray-head"
        else:
            container_name = "ray-worker"

        # logname
        stdout_log_path = f"{log_dir}/job-driver-{submission_id}.log"
        job_change_message["containerName"] = container_name
        job_change_message["logName"] = stdout_log_path
        hostip = node["nodeManagerAddress"]
        psm = dashboard_consts.get_global_psm()
        job_change_message["byted_log_url"] = generate_logagent_url(
            psm, hostip, node_name, container_name, stdout_log_path
        )

        if (
            ray_constants.RAY_ENABLE_DRIVER_ERR_LOG_FILE_ENVIRONMENT_VARIABLE
            in os.environ
        ):
            stderr_log_path = f"{log_dir}/job-driver-{submission_id}.err"
            job_change_message["errLogName"] = stderr_log_path
            job_change_message["byted_err_log_url"] = generate_logagent_url(
                psm, hostip, node_name, container_name, stderr_log_path
            )

    job_change_message["psm"] = dashboard_consts.get_global_psm()

    return job_change_message


# Reference to get_driver_jobs()
# deprecated because this job is not required for jobs
def add_driver_info_fields(reply, submission_id, job_change_message):
    from ray.dashboard.modules.job.pydantic_models import DriverInfo

    job_driver = None
    for job_table_entry in reply.job_info_list:
        if job_table_entry.config.ray_namespace.startswith(
            ray_constants.RAY_INTERNAL_NAMESPACE_PREFIX
        ):
            # Skip jobs in any _ray_internal_ namespace
            continue
        job_id = job_table_entry.job_id.hex()
        metadata = dict(job_table_entry.config.metadata)
        logger.info(job_table_entry)
        job_submission_id = metadata.get(JOB_ID_METADATA_KEY)
        # Ignore the job has no submission id
        # if not job_submission_id:
        #     continue
        if submission_id == job_submission_id:
            job_driver = DriverInfo(
                id=job_id,
                node_ip_address=job_table_entry.driver_address.ip_address,
                pid=job_table_entry.driver_pid,
            )
            break

    if job_driver:
        job_change_message["driver_info"] = job_driver.dict()
    else:
        logger.error(f"Can not find driver info for submission id: {submission_id}")

    return job_change_message

class HistoryServer:
    def __init__(
        self,
        address: str,
    ):
        self.gcs_address = address
        self.storage = create_history_server_storage()
        self._gcs_node_info_stub = None

    
    async def _listen_jobs(self):
        # Receive jobs from channel.
        gcs_addr = self.gcs_address
        logger.info(f"gcs address {gcs_addr}")
        subscriber = GcsAioJobSubmissionSubscriber(address=gcs_addr)
        await subscriber.subscribe()

        while True:
            logger.info("_listen_jobs from pubsub")
            msgs = await subscriber.poll(batch_size=200)
            for msg in msgs:
                # msg type: src.ray.protobuf.pubsub_pb2.PubMessage
                # append_job_event(msg['key_id'], msg['job_change_message'])
                submission_id = msg.key_id.decode("utf-8")
                job_change_message = json.loads(msg.job_change_message.json)
                #job_change_message = add_history_server_job_fields(
                #    submission_id, job_change_message
                #)
                logger.info(
                    f"_listen_jobs type: {type(job_change_message)} job_change_message: {job_change_message}"
                )
                append_job_event(self.storage, submission_id, job_change_message)
            await asyncio.sleep(5)

    async def _get_nodes(self):
        """Read the client table.

        Returns:
            A dict of information about the nodes in the cluster.
        """
        request = gcs_service_pb2.GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(request, timeout=2)
        if reply.status.code == 0:
            result = {}
            for node_info in reply.node_info_list:
                node_info_dict = gcs_node_info_to_dict(node_info)
                result[node_info_dict["nodeId"]] = node_info_dict
            return result
        else:
            logger.error("Failed to GetAllNodeInfo: %s", reply.status.message)

    async def _listen_nodes(self):
        gcs_channel = GcsChannel(gcs_address=self.gcs_address, aio=True)
        gcs_channel.connect()
        aiogrpc_gcs_channel = gcs_channel.channel()
        self._gcs_node_info_stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(
            aiogrpc_gcs_channel
        )
        # TODO(fyrestone): Refactor code for updating actor / node / job.
        # Subscribe actor channel.
        while True:
            try:
                logger.info("_listen_nodes from pubsub")
                nodes = await self._get_nodes()

                # update history server storage iff node state has changed
                logger.info(f"_listen_nodes DataSource.nodes: {DataSource.nodes}")
                logger.info(f"_listen_nodes nodes: {nodes}")
                for node_id, node in nodes.items():
                    if (
                        node_id not in DataSource.nodes
                        or node["state"] != DataSource.nodes[node_id]["state"]
                    ):
                        logger.info("_listen_nodes append")
                        append_node_event(self.storage, node_id, node)
                DataSource.nodes.reset(nodes)
            except Exception:
                logger.exception("Error listen nodes.")
            finally:
                await asyncio.sleep(5)

    async def _listen_actors(self):
        state_keys = (
            "state",
            "address",
            "numRestarts",
            "timestamp",
            "pid",
            "exitDetail",
            "startTime",
            "endTime",
            "reprName",
        )

        def process_actor_data_from_pubsub(actor_id, actor_table_data):
            actor_table_data = actor_table_data_to_dict(actor_table_data)
            # If actor is not new registered but updated, we only update
            # states related fields.
            if actor_table_data["state"] != "DEPENDENCIES_UNREADY":
                actors = DataSource.actors[actor_id]
                for k in state_keys:
                    if k in actor_table_data:
                        actors[k] = actor_table_data[k]
                actor_table_data = actors
            actor_id = actor_table_data["actorId"]
            node_id = actor_table_data["address"]["rayletId"]
            # Update actors.
            DataSource.actors[actor_id] = actor_table_data
            # Update node actors (only when node_id is not Nil).
            if node_id != actor_consts.NIL_NODE_ID:
                node_actors = DataSource.node_actors.get(node_id, {})
                node_actors[actor_id] = actor_table_data
                DataSource.node_actors[node_id] = node_actors
            return actor_table_data

        # Receive actors from channel.
        subscriber = GcsAioActorSubscriber(address=self.gcs_address)
        await subscriber.subscribe()

        while True:
            try:
                logger.info('_listen_actors from pubsub')
                published = await subscriber.poll(batch_size=200)
                actor_table_data_batch = []
                for actor_id, actor_table_data in published:
                    if actor_id is not None:
                        # Convert to lower case hex ID.
                        actor_id = actor_id.hex()
                        actor_table_data = process_actor_data_from_pubsub(actor_id, actor_table_data)
                        actor_table_data_batch.append(actor_table_data)
                append_actor_events(self.storage, actor_table_data_batch)
            except Exception:
                logger.exception("Error processing actor info from GCS.")

    async def _listen_tasks(self):
        # Receive actors from channel.
        subscriber = GcsAioTaskEventsSubscriber(address=self.gcs_address)
        await subscriber.subscribe()

        while True:
            try:
                logger.info('_listen_tasks from pubsub')
                published = await subscriber.poll(batch_size=200)
                actor_table_data_batch = []
                for task_id, task_events in published:
                    if task_id is not None:
                        # Convert to lower case hex ID.
                        task_id = task_id.hex()
                        logger.info(f'_listen_tasks task_id: {task_id} type: {type(task_events)} task_events: {task_events}')

                        orig_message = dashboard_utils.message_to_dict(task_events)
                        logger.info(f'_listen_tasks orig_message: {orig_message}')
                #append_actor_events(self.storage, actor_table_data_batch)
            except Exception:
                logger.exception("Error processing task info from GCS.")


    async def _run(self):
        await asyncio.gather(self._listen_jobs(), self._listen_nodes(), self._listen_actors(), self._listen_tasks())

    def run(self):
        loop = ray._private.utils.get_or_create_event_loop()
        loop.run_until_complete(self._run())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse GCS server for the monitor to connect to.")
    )
    parser.add_argument(
        "--gcs-address", required=False, type=str, help="The address (ip:port) of GCS."
    )
    parser.add_argument(
        "--logging-level",
        required=False,
        type=str,
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP,
    )
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP,
    )
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=ray_constants.MONITOR_LOG_FILE_NAME,
        help="Specify the name of log file, "
        "log to stdout if set empty, default is "
        f'"{ray_constants.MONITOR_LOG_FILE_NAME}"',
    )
    parser.add_argument(
        "--logs-dir",
        required=True,
        type=str,
        help="Specify the path of the temporary directory used by Ray processes.",
    )

    args = parser.parse_args()
    setup_component_logger(
        logging_level=args.logging_level,
        logging_format=args.logging_format,
        log_dir=args.logs_dir,
        filename=args.logging_filename,
        max_bytes=1000000,
        backup_count=1000000,
    )

    logger.info(f"Starting history server monitor using ray installation: {ray.__file__}")
    logger.info(f"Ray version: {ray.__version__}")
    logger.info(f"Ray commit: {ray.__commit__}")
    logger.info(f"Monitor started with command: {sys.argv}")

    bootstrap_address = args.gcs_address
    if bootstrap_address is None:
        raise ValueError("--gcs-address must be set!")

    history_server = HistoryServer(
        bootstrap_address,
    )

    history_server.run()
