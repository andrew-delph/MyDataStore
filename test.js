import { sleep } from "k6";
import http from "k6/http";
import { check } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";

export let options = {
  vus: 100,
  duration: "30s",
};

const address = "192.168.49.2:30000"; // localhost:80

export default function () {
  // Add a value to the map
  let serverRes = http.get(`http://${address}`);

  console.log("serverRes:", serverRes.body);

  // the key value to insert
  const key = randomString(5);
  const value = randomString(5);

  // Add a value to the map
  let addRes = http.get(`http://${address}/add?key=${key}&value=${value}`);

  check(addRes, {
    "Base: status was 200": (r) => r.status === 200,
  });

  console.log("addRes:", addRes.body);

  sleep(20);

  // Get a value from the map
  let getRes = http.get(`http://${address}/get?key=${key}`);
  check(getRes, {
    "Get: status was 200": (r) => r.status === 200,
    "Get: body contains testValue": (r) => r.body.indexOf(value) !== -1,
  });
  console.log("getRes:", getRes.body);

  // // List all values from the map
  // let listRes = http.get(`http://${address}/list`);

  // console.log("listRes:", listRes.body);
  // check(listRes, {
  //   "List: status was 200": (r) => r.status === 200,
  // });
}
