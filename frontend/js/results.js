const RESULTS_KEY = "medicoreSimulationResults";

function saveSimulationResults(results) {
    results.generatedAt = new Date().toISOString();

    localStorage.setItem(
        RESULTS_KEY,
        JSON.stringify(results)
    );
}

function loadSimulationResults() {
    const savedResults =
        localStorage.getItem(RESULTS_KEY);

    if (!savedResults) {
        return null;
    }

    try {
        return JSON.parse(savedResults);
    } catch (error) {
        console.error(
            "Could not load simulation results:",
            error
        );

        return null;
    }
}

function clearSimulationResults() {
    localStorage.removeItem(RESULTS_KEY);
}