
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

window.addEventListener("DOMContentLoaded", () => {
    const userQuestion = getUserQuestionFromLocalStorage();

    if (userQuestion) {
        writeUserQuestionToChatPage(userQuestion);
        localStorage.removeItem("userQuestion");
    }

});