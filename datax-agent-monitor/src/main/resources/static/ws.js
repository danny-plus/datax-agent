// 设置 STOMP 客户端
var stompClient = null;
// 设置 WebSocket 进入端点
var SOCKET_ENDPOINT = "/datax-agent-monitor/ws";
// 设置订阅消息的请求前缀
var SUBSCRIBE_PREFIX = "/topic/"
// 设置订阅消息的请求地址
var SUBSCRIBE = "";

/* 进行连接 */
function connect() {
    // 设置 SOCKET
    $("#connectBtn").addClass("disabled")

    var socket = new SockJS(SOCKET_ENDPOINT);
    // 配置 STOMP 客户端
    stompClient = Stomp.over(socket);
    // STOMP 客户端连接
    stompClient.connect({}, function (frame) {
        alert("连接成功");
        $("#disConnectBtn").removeClass("disabled")
        $("#subBtn").removeClass("disabled")

    });
}

var SUBSCRIBE = ""

/* 订阅信息 */
function subscribeSocket(){
    // 设置订阅地址
    SUBSCRIBE = SUBSCRIBE_PREFIX + $("#subscribe").val();
    // 输出订阅地址
    alert("设置订阅地址为：" + SUBSCRIBE);
    $("#subBtn").addClass("disabled")
    $("#unSubBtn").removeClass("disabled")
    // 执行订阅消息
    stompClient.subscribe(SUBSCRIBE, function (responseBody) {
        //var receiveMessage = JSON.parse(responseBody.body);
        $("#information").append("<tr><td>" + responseBody.body + "</td></tr>");
        console.log("now scrollHeight=>"+document.getElementById("information").scrollHeight)
        document.getElementById("information").scrollTop = document.getElementById("information").scrollHeight;
    });
}

function unSubscribeSocket(){
    stompClient.unsubscribe()
    SUBSCRIBE=""
    $("#subBtn").removeClass("disabled")
    $("#unSubBtn").addClass("disabled")
    $("#subscribe").val("");
    alert("解除订阅成功");

}

/* 断开连接 */
function disconnect() {
    $("#subBtn").addClass("disabled")
    $("#disConnectBtn").addClass("disabled")
    stompClient.disconnect(function() {
        alert("断开连接");
        $("#connectBtn").removeClass("disabled")
    });
}