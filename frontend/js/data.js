const hospitalProcesses = [
    {
        id: "P1",
        name: "ECG Alert - Bed 12",
        queue: "Emergency",
        arrival: 0,
        burst: 3,
        priority: 1
    },
    {
        id: "P2",
        name: "ICU Monitor - Bed 04",
        queue: "Emergency",
        arrival: 6,
        burst: 2,
        priority: 1
    },
    {
        id: "P3",
        name: "Code Blue Alert - ER",
        queue: "Emergency",
        arrival: 10,
        burst: 2,
        priority: 0
    },
    {
        id: "P4",
        name: "Doctor Dashboard",
        queue: "Interactive",
        arrival: 0,
        burst: 10,
        priority: 5
    },
    {
        id: "P5",
        name: "EMR Lookup",
        queue: "Interactive",
        arrival: 1,
        burst: 8,
        priority: 5
    },
    {
        id: "P6",
        name: "Nurse Chart Update",
        queue: "Interactive",
        arrival: 3,
        burst: 6,
        priority: 5
    },
    {
        id: "P7",
        name: "Billing Batch Job",
        queue: "Background",
        arrival: 0,
        burst: 14,
        priority: null
    },
    {
        id: "P8",
        name: "Nightly DB Backup",
        queue: "Background",
        arrival: 2,
        burst: 12,
        priority: 9
    }
];