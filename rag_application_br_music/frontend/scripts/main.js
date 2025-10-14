
function goToChatPage() {
    window.location.href = "chat.html";
}

function readUserQuestionFromMainPage() {
    const questionInputElement = document.querySelector(".js-input-field");
    const userQuestion = questionInputElement.value.trim();

    return userQuestion;
}

function saveUserQuestionToLocalStorage(userQuestion) {
    localStorage.setItem("userQuestion", userQuestion);
}

function getUserQuestionFromLocalStorage() {
    return localStorage.getItem("userQuestion") || "";
}

function writeUserQuestionToChatPage(userQuestion) {
    const userQuestionElement = document.querySelector(".js-user-question-text");
    console.log(userQuestionElement);

    if ( !userQuestionElement ){
        return;
    }

    userQuestionElement.textContent = userQuestion;
}

function userQuestionFlow() {
    const userQuestion = readUserQuestionFromMainPage();

    if (userQuestion === "") {
        return;
    }

    saveUserQuestionToLocalStorage(userQuestion);
    goToChatPage();
}


function requestChatResponse(userQuestion) {
    response = `This is a mock response for  the question.

    Question: ${userQuestion}
    `;

    // create the <p> elements for each \n

    response = response.split("\n").map( line => `<p>${line}</p>` ).join("");
    return response;
}

function requestChatRAGResponse(userQuestion) {
    response = `This is a mock response for  the question.

    Question: ${userQuestion}
    `;

    // create the <p> elements for each \n

    response = response.split("\n").map( line => `<p>${line}</p>` ).join("");
    return response;
}


function userQuestionResponseFlow() {
    const userQuestion = getUserQuestionFromLocalStorage();

    if (!userQuestion){
        return;
    }

    writeUserQuestionToChatPage(userQuestion);

    const chatResponse = requestChatResponse(userQuestion);
    const chatRAGResponse = requestChatRAGResponse(userQuestion);

    const chatReponseElement = document.querySelector(".js-chat-simple-response-text");
    const chatRAGResopnseElement = document.querySelector(".js-chat-rag-response-text");

    // include the responses 
    chatReponseElement.innerHTML = chatResponse;
    chatRAGResopnseElement.innerHTML = chatRAGResponse;

    localStorage.removeItem("userQuestion");
}

window.addEventListener("DOMContentLoaded", userQuestionResponseFlow);