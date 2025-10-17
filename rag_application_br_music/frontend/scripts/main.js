
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

function formatTextIntoParagraphs(text){
    return text.split("\n").map( line => `<p>${line}</p>` ).join("");
}


async function requestResponse(userQuestion) {
    const response = await fetch(`http://localhost:8000/generate-response?question=${userQuestion}`);
    const data = await response.json();
    return data
}

function userQuestionResponseFlow() {
    const userQuestion = getUserQuestionFromLocalStorage();

    if (!userQuestion){
        return;
    }

    requestResponse(userQuestion).then( 
        data => {
            const chatReponseElement = document.querySelector(".js-chat-simple-response-text");
            const chatRAGResopnseElement = document.querySelector(".js-chat-rag-response-text");
            const normalResponse = data.normal_response.content;
            const ragResponse = data.rag_response.content;

            console.log(data)
            
            // include the responses 
            chatReponseElement.innerHTML = formatTextIntoParagraphs(normalResponse);
            chatRAGResopnseElement.innerHTML = formatTextIntoParagraphs(ragResponse);
        }
    )

    writeUserQuestionToChatPage(userQuestion);
    localStorage.removeItem("userQuestion");
}

window.addEventListener("DOMContentLoaded", userQuestionResponseFlow);