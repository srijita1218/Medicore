function setupNavigation() {
    const currentPage = window.location.pathname.split("/").pop();

    document.querySelectorAll("[data-page]").forEach(link => {
        const targetPage = link.getAttribute("href").split("/").pop();

        if (targetPage === currentPage) {
            link.classList.add("active");
        }
    });
}

document.addEventListener("DOMContentLoaded", setupNavigation);

function setupNavigation() {

    const currentPage = window.location.pathname.split("/").pop();

    document.querySelectorAll(".main-nav a").forEach(link => {

        const linkPage = link.getAttribute("href").split("/").pop();

        if (linkPage === currentPage) {
            link.classList.add("active");
        }

    });

}

document.addEventListener("DOMContentLoaded", setupNavigation);