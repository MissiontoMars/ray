"""HistoryServer monitoring loop daemon."""

import argparse
import ray
import sys
import json
import asyncio
import logging
from ray._private.ray_logging import setup_component_logger
from ray._private.gcs_pubsub import _AioSubscriber
from ray.core.generated import pubsub_pb2
from typing import Tuple, List
import ray._private.ray_constants as ray_constants
import grpc
from storage import (
    append_job_event,
    create_history_server_storage,
)

logger = logging.getLogger(__name__)


class GcsAioJobSubmissionSubscriber(_AioSubscriber):
    def __init__(
        self,
        worker_id: bytes = None,
        address: str = None,
        channel: grpc.Channel = None,
    ):
        super().__init__(
            pubsub_pb2.RAY_JOB_SUBMISSION_STATE_CHANGE, worker_id, address, channel
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
                append_job_event(
                    self.storage,
                    submission_id,
                    job_change_message,
                )
            await asyncio.sleep(5)

    async def _run(self):
        await asyncio.gather(self._listen_jobs())

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

    logger.info(f"Starting monitor using ray installation: {ray.__file__}")
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
