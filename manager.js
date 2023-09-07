import http from "k6/http";
import { check, sleep } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.2/index.js";

export default function () {
  const key = randomString(10);
  const value = randomString(10);
  const setRes = http.get(
    `http://localhost:8080/set?key=${key}&value=${value}`
  );

  check(setRes, {
    "set status is 200": (r) => r.status === 200,
  });
  // sleep(2);

  const getRes = http.get(`http://localhost:8080/get?key=${key}`);

  check(getRes, {
    "get status is 200": (r) => r.status === 200,
    "get the correct value": (r) => r.body === value || console.error(r.body),
  });
}

export const options = {
  vus: 100,
  duration: "10s",
  summaryTrendStats: ["avg", "min", "med", "max"],
  // summaryTimeUnit: "ms",
};

export function handleSummary(data) {
  for (const key in data.metrics) {
    if (!key.includes("duration")) delete data.metrics[key];
  }

  return {
    stdout: textSummary(data, { indent: "→", enableColors: true }),
  };
}
