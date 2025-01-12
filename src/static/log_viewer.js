const logDiv = document.getElementById("log");
const socket = new WebSocket("ws://127.0.0.1:8000/ws");

socket.onmessage = (event) => {
  logDiv.innerHTML += event.data;
  logDiv.scrollTop = logDiv.scrollHeight;
};

socket.onclose = () => {
  logDiv.textContent += "\n[Connection closed]";
};
