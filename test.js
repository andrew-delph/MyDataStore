import http from "k6/http";
import { check } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";

export let options = {
  // vus: 10, // 10 virtual users
  // duration: "30s", // 30-second test
};

export default function () {
  // the key value to insert
  const key = randomString(5);
  const value = randomString(5);

  // Add a value to the map
  let addRes = http.get(`http://localhost:8080/add?key=${key}&value=${value}`);
  check(addRes, {
    "Add: status was 200": (r) => r.status === 200,
  });
  console.log("addRes:", addRes.body);

  // Get a value from the map
  let getRes = http.get(`http://localhost:8080/get?key=${key}`);
  check(getRes, {
    "Get: status was 200": (r) => r.status === 200,
    "Get: body contains testValue": (r) => r.body.indexOf(value) !== -1,
  });
  console.log("getRes:", getRes.body);

  // List all values from the map
  let listRes = http.get(`http://localhost:8080/list`);

  console.log("listRes:", listRes.body);
  check(listRes, {
    "List: status was 200": (r) => r.status === 200,
  });
}
