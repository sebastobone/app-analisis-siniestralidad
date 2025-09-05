import { showToast } from "./toast.js";

const BASE_URL = "http://127.0.0.1:8000";

async function handleRequest(request, errorMsg) {
  try {
    const response = await request();

    if (!response.ok) {
      throw new Error(errorMsg);
    }

    const data = await response.json();
    showToast(data.message, "success");
    return data;
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}

export function fetchForm(formId, endpoint, errorMsg) {
  const formData = new URLSearchParams(
    new FormData(document.getElementById(formId)),
  );

  return handleRequest(
    () =>
      fetch(`${BASE_URL}/${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: formData.toString(),
      }),
    errorMsg,
  );
}

export function fetchSimple(endpoint, errorMsg) {
  return handleRequest(
    () =>
      fetch(`${BASE_URL}/${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      }),
    errorMsg,
  );
}
