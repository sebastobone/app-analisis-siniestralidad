var source = new EventSource("http://127.0.0.1:8000/stream-logs");
source.onmessage = function (event) {
  document.getElementById("log").innerHTML += event.data;
};
