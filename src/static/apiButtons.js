document.querySelectorAll(".apiButton").forEach((button) => {
  button.addEventListener("click", async function (event) {
    event.preventDefault();

    const endpoint = button.getAttribute("endpoint");
    await fetch(`http://127.0.0.1:8000/${endpoint}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    });
  });
});
