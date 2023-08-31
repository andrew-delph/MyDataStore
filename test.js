// import { PodDisruptor } from "k6/x/disruptor";
import { sleep } from "k6";
import http from "k6/http";
import { check } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.2/index.js";

export let options = {
  // iterations: 20,
  // vus: 5,
  scenarios: {
    // load: {
    //   executor: "constant-vus",
    //   vus: 1,
    //   duration: "5m",
    //   exec: "default",
    // },
    constant_arrival: {
      executor: "constant-arrival-rate",
      // How long the test lasts
      duration: "2h",
      // How many iterations per timeUnit
      rate: 10,
      // Start `rate` iterations per second
      timeUnit: "1s",
      // Pre-allocate VUs
      preAllocatedVUs: 1,
      maxVUs: 500,
    },
    // ramping_arrival: {
    //   executor: "ramping-arrival-rate",
    //   startRate: 50,
    //   stages: [
    //     { target: 75, duration: "2m" },
    //     { target: 100, duration: "1h" },
    //   ],
    //   // Start `rate` iterations per second
    //   timeUnit: "1s",
    //   // Pre-allocate VUs
    //   preAllocatedVUs: 50,
    //   maxVUs: 10000,
    // },
    // epoch: {
    //   executor: "constant-arrival-rate",
    //   exec: "epoch",
    //   // How long the test lasts
    //   duration: "4m",
    //   // How many iterations per timeUnit
    //   rate: 1,
    //   // Start `rate` iterations per second
    //   timeUnit: "10s",
    //   // Pre-allocate VUs
    //   preAllocatedVUs: 1,
    //   maxVUs: 1,
    // },
  },
};

let address = "192.168.49.2:30000";
// address = "localhost:8080";

// export function handleSummary(data) {
//   let output = data;
//   delete output.metrics;
//   return {
//     stdout: textSummary(output, { indent: " ", enableColors: true }),
//   };
// }

export function basic() {
  let addr = `http://${address}`;
  let baseRes = http.get(addr);
  check(baseRes, {
    "Basic: status was 200": (r) =>
      r.status === 200 || console.error(`Base Error: Status was ${r.status}`),
  });

  return;
}

export function panic() {
  let addr = `http://${address}/panic`;
  let baseRes = http.get(addr);
  check(baseRes, {
    "Panic: status was 200": (r) =>
      r.status === 200 || console.error(`Base Error: Status was ${r.status}`),
  });

  return;
}

export function leader() {
  let addr = `http://${address}/leader`;
  let baseRes = http.get(addr);
  check(baseRes, {
    "Leader: status was 200": (r) =>
      r.status === 200 || console.error(`Base Error: Status was ${r.status}`),
  });

  return;
}

export function follower() {
  let addr = `http://${address}/follower`;
  console.log(addr);
  let baseRes = http.get(addr);
  check(baseRes, {
    "Follower: status was 200": (r) =>
      r.status === 200 || console.error(`Base Error: Status was ${r.status}`),
  });

  return;
}

export function bootstrap() {
  let addr = `http://${address}/bootstrap`;

  let baseRes = http.get(addr);
  check(baseRes, {
    "bootstrap: status was 200": (r) =>
      r.status === 200 ||
      console.error(`bootstrap Error: Status was ${r.status}`),
  });

  return;
}

export function epoch() {
  let addr = `http://${address}/epoch`;

  let baseRes = http.get(addr);
  check(baseRes, {
    "epoch: status was 200": (r) =>
      r.status === 200 || console.error(`epoch Error: Status was ${r.status}`),
  });
  return;
}

export function remove() {
  let addr = `http://${address}/remove`;

  let baseRes = http.get(addr);
  check(baseRes, {
    "remove: status was 200": (r) =>
      r.status === 200 || console.error(`remove Error: Status was ${r.status}`),
  });
  return;
}

options = { duration: "2h", vus: 1 };
// options = { iterations: 1, vus: 1 };
export default function () {
  // leader();
  // return;

  // the key value to insert
  let key = randomString(15);
  let value = randomString(15);

  // Set a value to the map
  let setRes = http.get(`http://${address}/set?key=${key}&value=${value}`);

  check(setRes, {
    "Set: status was 200": (r) => r.status === 200,
  });
  if (setRes.status != 200) {
    console.error(`Set Error: status= ${setRes.status} body= ${setRes.body}`);
  }

  return;

  sleep(10);

  // Get a value from the map
  let getRes = http.get(`http://${address}/get?key=${key}`);

  check(getRes, {
    "Get: status was 200": (r) => r.status === 200,
    "Get: body contains testValue": (r) =>
      r.body && r.body.indexOf(value) !== -1,
  });

  if ((getRes.body || "").indexOf(value) == -1 || getRes.status != 200) {
    console.error(
      `Get Error: status= ${getRes.status} missing= ${
        (getRes.body || "").indexOf(value) == -1
      } body= ${getRes.body}`
    );
  }
}
