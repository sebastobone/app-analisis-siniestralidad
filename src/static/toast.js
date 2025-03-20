export function showToast(message, type = "info", duration = 3000) {
  const toastContainer =
    document.getElementById("toast-container") || createToastContainer();

  const toast = document.createElement("div");
  toast.classList.add("toast", `toast-${type}`);
  toast.innerText = message;

  toastContainer.appendChild(toast);

  setTimeout(() => {
    toast.classList.add("hide");
    setTimeout(() => toast.remove(), 500);
  }, duration);
}

function createToastContainer() {
  const container = document.createElement("div");
  container.id = "toast-container";
  document.body.appendChild(container);
  return container;
}
