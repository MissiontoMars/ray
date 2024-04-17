import useSWR from "swr";
import { PER_JOB_PAGE_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getActors, getHistoryActors } from "../../../service/actor";
import { useParams } from "react-router-dom";

export const useActorList = () => {
  const params = useParams() as { clustername: string};
  const { data } = useSWR(
    "useActorList",
    async () => {
      let rsp
      if ("clustername" in params) {
        rsp = await getHistoryActors(params.clustername);
      } else {
        rsp = await getActors();
      }

      if (rsp?.data?.data?.actors) {
        return rsp.data.data.actors;
      } else {
        return {};
      }
    },
    { refreshInterval: PER_JOB_PAGE_REFRESH_INTERVAL_MS },
  );

  return data;
};
