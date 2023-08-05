// import { PodDisruptor } from "k6/x/disruptor";
import { sleep } from "k6";
import http from "k6/http";
import { check } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";

export let options = {
  vus: 10,
  duration: "10s",

  // iterations: 30,
  // scenarios: {
  //   // disrupt: {
  //   //   executor: "shared-iterations",
  //   //   iterations: 1,
  //   //   vus: 1,
  //   //   exec: "disrupt",
  //   //   startTime: "0s",
  //   // },
  //   load: {
  //     executor: "constant-arrival-rate",
  //     rate: 100,
  //     preAllocatedVUs: 10,
  //     maxVUs: 1000,
  //     exec: "default",
  //     startTime: "0s", // give time for the agents to be installed
  //     duration: "20m",
  //   },
  // },
};

let address = "192.168.49.2:30000";
address = "localhost:80";

export default function () {
  // Get the id from the server
  let baseRes = http.get(`http://${address}`);

  check(baseRes, {
    "Base: status was 200": (r) => r.status === 200,
  });

  // the key value to insert
  const key = randomString(5);
  const value = randomString(5);

  // Add a value to the map
  let addRes = http.get(`http://${address}/add?key=${key}&value=${value}`);

  check(addRes, {
    "Add: status was 200": (r) => r.status === 200,
  });

  // sleep(10);

  // Get a value from the map
  let getRes = http.get(`http://${address}/get?key=${key}`);
  check(getRes, {
    "Get: status was 200": (r) => r.status === 200,
    "Get: body contains testValue": (r) => r.body.indexOf(value) !== -1,
  });

  // // List all values from the map
  // let listRes = http.get(`http://${address}/list`);

  // console.log("listRes:", listRes.body);
  // check(listRes, {
  //   "List: status was 200": (r) => r.status === 200,
  // });
}

// export function disrupt() {
//   console.log(1);
//   const disruptor = new PodDisruptor({
//     namespace: "default",
//     select: { labels: { "io.kompose.service": "store" } },
//   });

//   // Disrupt the targets by injecting HTTP faults into them for 30 seconds
//   const fault = {
//     averageDelay: "1s",
//     errorRate: 0.1,
//     errorCode: 500,
//   };
//   console.log(2);
//   disruptor.injectHTTPFaults(fault, "1s");
//   console.log(3);
// }
