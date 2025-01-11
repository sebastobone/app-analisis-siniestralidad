const logDiv = document.getElementById("log");
const socket = new WebSocket("ws://127.0.0.1:8000/ws");

socket.onmessage = (event) => {
  const lastLog = document.createElement("div");
  lastLog.innerHTML = event.data;
  logDiv.appendChild(lastLog);
  logDiv.scrollTop = logDiv.scrollHeight;
};

socket.onclose = () => {
  logDiv.textContent += "\n[Connection closed]";
};
