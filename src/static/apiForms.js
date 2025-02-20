document.querySelectorAll(".apiForm").forEach((form) => {
  form.addEventListener("submit", async function (event) {
    event.preventDefault();

    const endpoint = form.getAttribute("endpoint");
    await fetch(`http://127.0.0.1:8000/${endpoint}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    });
  });
});
