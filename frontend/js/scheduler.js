/* ============================================================
   Core scheduler engine — mirrors amq_scheduler.py
   ============================================================ */
const EMERGENCY = 0, INTERACTIVE = 1, BACKGROUND = 2;
const QUEUE_NAME = {
  0: 'Emergency',
  1: 'Interactive',
  2: 'Background'
};

const AGING_SLOPE_BG = 0.2;
const AGING_PROMOTION_BASE = 8;
const RR_QUANTUM = 4;

let nextPid = 1;

function makeProcess(name, queue, arrival, burst, basePriority) {
  return {
    pid: 'P' + (nextPid++),
    name,
    queue,
    arrival,
    burst,
    basePriority,
    remaining: burst,
    waitingSince: arrival,
    firstRun: null,
    completion: null,
    quantumLeft: RR_QUANTUM,
    promotions: 0,
  };
}

function cloneTasks(tasks) {
  nextPid = 1;

  return tasks.map(t =>
    makeProcess(
      t.name,
      t.queue,
      t.arrival,
      t.burst,
      t.basePriority
    )
  );
}

function newEngine(tasks) {
  return {
    all: [...tasks].sort((a, b) => a.arrival - b.arrival),
    queues: {
      0: [],
      1: [],
      2: []
    },
    time: 0,
    gantt: [],
    idle: 0,
    done: false,
  };
}

function flat(e) {
  return [
    ...e.queues[0],
    ...e.queues[1],
    ...e.queues[2]
  ];
}

function admitArrivals(e) {
  for (const p of e.all) {
    if (
      p.arrival === e.time &&
      p.completion === null &&
      !flat(e).includes(p) &&
      !isRunning(e, p)
    ) {
      e.queues[p.queue].push(p);
    }
  }
}

let runningRef = null;

function isRunning(e, p) {
  return runningRef === p;
}

function applyAging(e) {
  if (AGING_SLOPE_BG <= 0) return;

  const threshold =
    AGING_PROMOTION_BASE / AGING_SLOPE_BG;

  for (const p of [...e.queues[BACKGROUND]]) {
    if ((e.time - p.waitingSince) >= threshold) {
      e.queues[BACKGROUND] =
        e.queues[BACKGROUND].filter(x => x !== p);

      p.waitingSince = e.time;
      p.quantumLeft = RR_QUANTUM;
      p.promotions++;

      e.queues[INTERACTIVE].push(p);
    }
  }
}

function pickEmergency(e) {
  if (e.queues[0].length === 0) return null;

  e.queues[0].sort(
    (a, b) =>
      a.basePriority - b.basePriority ||
      a.arrival - b.arrival
  );

  return e.queues[0][0];
}

let current = null;
let currentQ = null;

function tickOnce(e) {
  if (e.done) return null;

  admitArrivals(e);
  applyAging(e);

  const emg = pickEmergency(e);

  let chosen = null;
  let chosenQ = null;

  if (emg) {
    chosen = emg;
    chosenQ = EMERGENCY;

    if (
      current &&
      current !== chosen &&
      !e.queues[currentQ].includes(current)
    ) {
      e.queues[currentQ].unshift(current);
    }

  } else if (e.queues[INTERACTIVE].length > 0) {

    chosen = e.queues[INTERACTIVE][0];
    chosenQ = INTERACTIVE;

  } else if (e.queues[BACKGROUND].length > 0) {

    chosen = e.queues[BACKGROUND][0];
    chosenQ = BACKGROUND;
  }

  if (!chosen) {
    e.idle++;
    e.time++;

    if (
      e.all.every(p => p.completion !== null)
    ) {
      e.done = true;
    }

    return {
      idle: true,
      time: e.time
    };
  }

  e.queues[chosenQ] =
    e.queues[chosenQ].filter(x => x !== chosen);

  runningRef = chosen;

  if (chosen.firstRun === null) {
    chosen.firstRun = e.time;
  }

  const startT = e.time;

  e.gantt.push([
    startT,
    startT + 1,
    chosen.pid,
    chosenQ,
    chosen.name
  ]);

  chosen.remaining--;
  e.time++;

  admitArrivals(e);

  let finished = false;

  if (chosen.remaining <= 0) {

    chosen.completion = e.time;
    finished = true;

    current = null;
    currentQ = null;
    runningRef = null;

  } else if (chosenQ === EMERGENCY) {

    chosen.waitingSince = e.time;
    e.queues[EMERGENCY].push(chosen);

    current = chosen;
    currentQ = EMERGENCY;
    runningRef = chosen;

  } else if (chosenQ === INTERACTIVE) {

    chosen.quantumLeft--;

    if (
      chosen.quantumLeft <= 0 ||
      pickEmergency(e)
    ) {

      chosen.quantumLeft = RR_QUANTUM;
      chosen.waitingSince = e.time;

      e.queues[INTERACTIVE].push(chosen);

      current = null;
      currentQ = null;
      runningRef = null;

    } else {

      e.queues[INTERACTIVE].unshift(chosen);

      current = chosen;
      currentQ = INTERACTIVE;
      runningRef = chosen;
    }

  } else {

    // BACKGROUND
    e.queues[BACKGROUND].unshift(chosen);

    current = chosen;
    currentQ = BACKGROUND;
    runningRef = chosen;
  }

  if (
    e.all.every(p => p.completion !== null)
  ) {
    e.done = true;
  }

  return {
    idle: false,
    time: e.time,
    pid: chosen.pid,
    name: chosen.name,
    queue: chosenQ,
    finished
  };
}

function report(e) {
  const rows = [];

  for (
    const p of [...e.all].sort(
      (a, b) =>
        a.pid.localeCompare(
          b.pid,
          undefined,
          { numeric: true }
        )
    )
  ) {

    const executed =
      p.burst - p.remaining;

    let turnaround = null;
    let waiting;
    let response;
    let starved;

    if (p.completion !== null) {

      turnaround =
        p.completion - p.arrival;

      waiting =
        turnaround - p.burst;

      response =
        p.firstRun !== null
          ? p.firstRun - p.arrival
          : null;

      starved = false;

    } else {

      waiting =
        (e.time - p.arrival) - executed;

      response =
        p.firstRun !== null
          ? p.firstRun - p.arrival
          : null;

      starved = true;
    }

    rows.push({
      pid: p.pid,
      name: p.name,
      queue: QUEUE_NAME[p.queue],
      arrival: p.arrival,
      burst: p.burst,
      completion: p.completion,
      turnaround,
      waiting,
      response,
      promotions: p.promotions,
      starved
    });
  }

  const util =
    e.time > 0
      ? 100 * (e.time - e.idle) / e.time
      : 0;

  return {
    rows,
    util
  };
}


/* ============================================================
   Plain FCFS baseline
   ============================================================ */

function runFcfsBaseline(tasks) {

  const procs =
    cloneTasks(tasks)
      .sort((a, b) => a.arrival - b.arrival);

  let t = 0;
  const rows = [];

  for (const p of procs) {

    const start =
      Math.max(t, p.arrival);

    const end =
      start + p.burst;

    p.firstRun = start;
    p.completion = end;

    t = end;

    rows.push({
      pid: p.pid,
      name: p.name,
      queue: QUEUE_NAME[p.queue],
      arrival: p.arrival,
      burst: p.burst,
      completion: end,
      turnaround: end - p.arrival,
      waiting:
        (end - p.arrival) - p.burst,
      response:
        start - p.arrival
    });
  }

  return {
    rows,
    totalTime: t
  };
}


/* ============================================================
   UI wiring
   ============================================================ */

const QCOLOR = {
  0: 'q0',
  1: 'q1',
  2: 'q2'
};

let userTasks = [];
let engine = null;
let playTimer = null;
let originalTasksForCompare = null;


function renderTaskList() {

  const el =
    document.getElementById('taskList');

  if (userTasks.length === 0) {

    el.innerHTML =
      '<div class="empty-note" style="padding:10px 0;">No tasks yet.</div>';

    return;
  }

  el.innerHTML =
    userTasks.map((t, i) => `
      <div class="task-row">

        <div
          class="bar"
          style="background:var(--${
            t.queue === 0
              ? 'emergency'
              : t.queue === 1
                ? 'interactive'
                : 'background'
          })">
        </div>

        <div class="info">

          <div class="name">
            ${t.name}

            <span class="queue-tag ${QCOLOR[t.queue]}">
              ${QUEUE_NAME[t.queue]}
            </span>
          </div>

          <div class="meta">
            arrival t=${t.arrival}
            · burst=${t.burst}
            · priority=${t.basePriority}
          </div>

        </div>

        <button
          data-i="${i}"
          class="rm">
          ×
        </button>

      </div>
    `).join('');

  el.querySelectorAll('.rm')
    .forEach(b =>
      b.addEventListener(
        'click',
        () => {
          userTasks.splice(+b.dataset.i, 1);
          renderTaskList();
        }
      )
    );
}


/* ============================================================
   Add task
   ============================================================ */

document
  .getElementById('btnAdd')
  .addEventListener('click', () => {

    const name =
      document
        .getElementById('fName')
        .value
        .trim() ||
      'Untitled task';

    const queue =
      +document.getElementById('fQueue').value;

    const priority =
      +document.getElementById('fPriority').value;

    const arrival =
      +document.getElementById('fArrival').value;

    const burst =
      +document.getElementById('fBurst').value;

    userTasks.push({
      name,
      queue,
      arrival,
      burst,
      basePriority: priority
    });

    document
      .getElementById('fName')
      .value = '';

    renderTaskList();
  });


/* ============================================================
   Clear tasks
   ============================================================ */

document
  .getElementById('btnClear')
  .addEventListener('click', () => {

    userTasks = [];

    renderTaskList();
    resetRun();
  });


/* ============================================================
   Hospital preset
   ============================================================ */

document
  .getElementById('btnPresetHospital')
  .addEventListener('click', () => {

    userTasks = [

      {
        name: 'ECG Alert - Bed 12',
        queue: 0,
        arrival: 0,
        burst: 3,
        basePriority: 1
      },

      {
        name: 'ICU Monitor - Bed 04',
        queue: 0,
        arrival: 6,
        burst: 2,
        basePriority: 1
      },

      {
        name: 'Code Blue Alert - ER',
        queue: 0,
        arrival: 10,
        burst: 2,
        basePriority: 0
      },

      {
        name: 'Doctor Dashboard',
        queue: 1,
        arrival: 0,
        burst: 10,
        basePriority: 5
      },

      {
        name: 'EMR Lookup',
        queue: 1,
        arrival: 1,
        burst: 8,
        basePriority: 5
      },

      {
        name: 'Nurse Chart Update',
        queue: 1,
        arrival: 3,
        burst: 6,
        basePriority: 5
      },

      {
        name: 'Billing Batch Job',
        queue: 2,
        arrival: 0,
        burst: 14,
        basePriority: 9
      },

      {
        name: 'Nightly DB Backup',
        queue: 2,
        arrival: 2,
        burst: 12,
        basePriority: 9
      }
    ];

    renderTaskList();
    resetRun();
  });


/* ============================================================
   Stress preset
   ============================================================ */

document
  .getElementById('btnPresetStress')
  .addEventListener('click', () => {

    const t = [];

    t.push({
      name: 'Background Job #1',
      queue: 2,
      arrival: 0,
      burst: 18,
      basePriority: 9
    });

    t.push({
      name: 'Background Job #2',
      queue: 2,
      arrival: 0,
      burst: 16,
      basePriority: 9
    });

    let at = 8;

    while (at < 90) {

      t.push({
        name: 'Emergency Alert',
        queue: 0,
        arrival: at,
        burst: 2,
        basePriority: 1
      });

      at += 30;
    }

    at = 0;

    while (at < 90) {

      t.push({
        name: 'Interactive Task',
        queue: 1,
        arrival: at,
        burst: 3,
        basePriority: 5
      });

      at += 4;
    }

    userTasks = t;

    renderTaskList();
    resetRun();
  });


/* ============================================================
   Reset
   ============================================================ */

function resetRun() {

  clearInterval(playTimer);

  playTimer = null;
  engine = null;
  current = null;
  currentQ = null;
  runningRef = null;

  document
    .getElementById('statusPill')
    .textContent = 'IDLE';

  document
    .getElementById('statusPill')
    .className = 'status-pill';

  document
    .getElementById('ganttWrap')
    .innerHTML =
      '<div class="empty-note">Add tasks (or load a preset) and press Run.</div>';

  document
    .getElementById('nowBadge')
    .textContent = '— idle —';

  document
    .getElementById('nowBadge')
    .className = 'badge';

  document
    .getElementById('tickLabel')
    .textContent = 't = 0';

  document
    .getElementById('resultsPanel')
    .style.display = 'none';

  document
    .getElementById('comparePanel')
    .style.display = 'none';

  [
    'tElapsed',
    'tUtil',
    'tWait',
    'tEmerg'
  ].forEach(id =>
    document.getElementById(id).textContent = '0'
  );

  document
    .getElementById('tBg')
    .textContent = 'OK';

  document
    .getElementById('tileBg')
    .className = 'tile';
}


document
  .getElementById('btnReset')
  .addEventListener('click', resetRun);


/* ============================================================
   Start engine
   ============================================================ */

function startEngine() {

  if (userTasks.length === 0) {

    alert(
      'Add at least one task or load a preset first.'
    );

    return null;
  }

  originalTasksForCompare =
    JSON.parse(
      JSON.stringify(userTasks)
    );

  const procs =
    cloneTasks(userTasks);

  const e =
    newEngine(procs);

  document
    .getElementById('ganttWrap')
    .innerHTML =
      buildGanttSkeleton(procs);

  document
    .getElementById('resultsPanel')
    .style.display = 'none';

  document
    .getElementById('comparePanel')
    .style.display = 'none';

  return e;
}


/* ============================================================
   Gantt
   ============================================================ */

function buildGanttSkeleton(procs) {

  const rows =
    procs.map(p => `
      <div
        class="gantt-row"
        data-pid="${p.pid}">

        <div class="pid-label">
          ${p.pid}
        </div>

        <div
          class="gantt-track"
          id="track-${p.pid}">
        </div>

      </div>
    `).join('');

  return (
    rows +
    '<div class="gantt-axis" id="ganttAxis"></div>'
  );
}


const PX_PER_TICK = 14;


/* ============================================================
   Paint tick
   ============================================================ */

function paintTick(e, info) {

  document
    .getElementById('tickLabel')
    .textContent =
      't = ' + e.time;

  const badge =
    document.getElementById('nowBadge');

  if (info && !info.idle) {

    badge.textContent =
      info.name +
      ' (' +
      QUEUE_NAME[info.queue] +
      ')';

    badge.className =
      'badge ' +
      QCOLOR[info.queue];

    const track =
      document.getElementById(
        'track-' + info.pid
      );

    if (track) {

      const block =
        document.createElement('div');

      block.className =
        'gantt-block ' +
        QCOLOR[info.queue];

      block.style.left =
        (e.time - 1) *
        PX_PER_TICK +
        'px';

      block.style.width =
        (PX_PER_TICK - 1) +
        'px';

      track.appendChild(block);

      track.style.width =
        e.time *
        PX_PER_TICK +
        'px';
    }

  } else {

    badge.textContent =
      '— idle —';

    badge.className =
      'badge';
  }


  const axis =
    document.getElementById('ganttAxis');

  if (
    axis &&
    e.time % 5 === 0
  ) {

    axis.style.width =
      e.time *
      PX_PER_TICK +
      'px';

    axis.textContent =
      'now → t=' +
      e.time;
  }


  document
    .getElementById('ganttWrap')
    .scrollLeft =
      e.time *
      PX_PER_TICK;


  /* ==========================================================
     Live tiles
     ========================================================== */

  const {
    rows,
    util
  } = report(e);

  const finishedRows =
    rows.filter(
      r => r.completion !== null
    );

  const avgWait =
    finishedRows.length
      ? finishedRows.reduce(
          (s, r) => s + r.waiting,
          0
        ) / finishedRows.length
      : 0;

  const emgRows =
    finishedRows.filter(
      r => r.queue === 'Emergency'
    );

  const avgEmg =
    emgRows.length
      ? emgRows.reduce(
          (s, r) => s + r.response,
          0
        ) / emgRows.length
      : 0;

  const bgRows =
    rows.filter(
      r => r.queue === 'Background'
    );

  const bgPromotions =
    bgRows.reduce(
      (s, r) => s + r.promotions,
      0
    );

  const bgStillWaiting =
    bgRows.some(
      r => r.completion === null
    );


  document
    .getElementById('tElapsed')
    .textContent =
      e.time;

  document
    .getElementById('tUtil')
    .textContent =
      util.toFixed(0);

  document
    .getElementById('tWait')
    .textContent =
      avgWait.toFixed(1);

  document
    .getElementById('tEmerg')
    .textContent =
      avgEmg.toFixed(1);


  const tileBg =
    document.getElementById('tileBg');

  const tBg =
    document.getElementById('tBg');


  if (
    bgStillWaiting &&
    bgPromotions === 0
  ) {

    tBg.textContent = 'WAITING';
    tileBg.className = 'tile';

  } else if (
    bgPromotions > 0
  ) {

    tBg.textContent =
      'AGED ×' +
      bgPromotions;

    tileBg.className =
      'tile good';

  } else {

    tBg.textContent = 'OK';
    tileBg.className =
      'tile good';
  }
}


/* ============================================================
   FINALIZE RUN
   ============================================================ */

function finalizeRun(e) {

  const {
    rows,
    util
  } = report(e);


  /* ==========================================================
     Display results table
     ========================================================== */

  document
    .getElementById('resultsPanel')
    .style.display = 'block';

  const tbl =
    document.getElementById(
      'resultsTable'
    );

  tbl.innerHTML = `
    <tr>
      <th>PID</th>
      <th>Name</th>
      <th>Queue</th>
      <th>Arr</th>
      <th>Burst</th>
      <th>Compl.</th>
      <th>Wait</th>
      <th>Resp.</th>
      <th>Promotions</th>
    </tr>
  ` +
  rows.map(r => `
    <tr>
      <td>${r.pid}</td>
      <td>${r.name}</td>
      <td>${r.queue}</td>
      <td>${r.arrival}</td>
      <td>${r.burst}</td>
      <td>
        ${
          r.completion !== null
            ? r.completion
            : '—'
        }
      </td>
      <td>${r.waiting}</td>
      <td>
        ${
          r.response !== null
            ? r.response
            : '—'
        }
      </td>
      <td>${r.promotions}</td>
    </tr>
  `).join('');


  /* ==========================================================
     FCFS baseline comparison
     ========================================================== */

  const fcfs =
    runFcfsBaseline(
      originalTasksForCompare
    );

  const amqEmg =
    rows.filter(
      r =>
        r.queue === 'Emergency' &&
        r.response !== null
    );

  const fcfsEmg =
    fcfs.rows.filter(
      r => r.queue === 'Emergency'
    );


  const amqEmgAvg =
    amqEmg.length
      ? amqEmg.reduce(
          (s, r) => s + r.response,
          0
        ) / amqEmg.length
      : 0;


  const fcfsEmgAvg =
    fcfsEmg.length
      ? fcfsEmg.reduce(
          (s, r) => s + r.response,
          0
        ) / fcfsEmg.length
      : 0;


  const amqWaitAvg =
    rows.length
      ? rows.reduce(
          (s, r) => s + r.waiting,
          0
        ) / rows.length
      : 0;


  const fcfsWaitAvg =
    fcfs.rows.length
      ? fcfs.rows.reduce(
          (s, r) => s + r.waiting,
          0
        ) / fcfs.rows.length
      : 0;


  const completedRows =
    rows.filter(
      r => r.turnaround !== null
    );


  const amqTatAvg =
    completedRows.length
      ? completedRows.reduce(
          (s, r) => s + r.turnaround,
          0
        ) / completedRows.length
      : 0;


  const fcfsTatAvg =
    fcfs.rows.length
      ? fcfs.rows.reduce(
          (s, r) => s + r.turnaround,
          0
        ) / fcfs.rows.length
      : 0;


  /* ==========================================================
     Average response time
     ========================================================== */

  const responseRows =
    rows.filter(
      r => r.response !== null
    );


  const amqResponseAvg =
    responseRows.length
      ? responseRows.reduce(
          (s, r) => s + r.response,
          0
        ) / responseRows.length
      : 0;


  /* ==========================================================
     Comparison metrics
     ========================================================== */

  const metrics = [
    {
      label:
        'Emergency response time (ticks)',
      fcfs: fcfsEmgAvg,
      amq: amqEmgAvg
    },

    {
      label:
        'Average waiting time (ticks)',
      fcfs: fcfsWaitAvg,
      amq: amqWaitAvg
    },

    {
      label:
        'Average turnaround time (ticks)',
      fcfs: fcfsTatAvg,
      amq: amqTatAvg
    }
  ];


  /* ==========================================================
     SAVE RESULTS
     ========================================================== */

  saveSimulationResults({

    metrics: {

      averageWaiting:
        amqWaitAvg,

      averageTurnaround:
        amqTatAvg,

      averageResponse:
        amqResponseAvg,

      cpuUtilization:
        util,

      emergencyResponse:
        amqEmgAvg
    },

    gantt:
      e.gantt,

    processes:
      rows,

    processCount:
      rows.length
  });


  /* ==========================================================
     Render AMQ vs FCFS
     ========================================================== */

  const maxVal =
    Math.max(
      ...metrics.map(
        m =>
          Math.max(
            m.fcfs,
            m.amq
          )
      ),
      1
    );


  document
    .getElementById('comparePanel')
    .style.display = 'block';


  document
    .getElementById('compareBars')
    .innerHTML =
      metrics.map(m => `
        <div class="compare-metric">

          <div class="label">
            ${m.label}
          </div>

          <div class="bar-line">

            <div class="name">
              FCFS
            </div>

            <div class="track">
              <div
                class="fill fcfs"
                style="width:${
                  (
                    m.fcfs /
                    maxVal *
                    100
                  ).toFixed(1)
                }%">
              </div>
            </div>

            <div class="num">
              ${m.fcfs.toFixed(1)}
            </div>

          </div>


          <div class="bar-line">

            <div class="name">
              AMQ
            </div>

            <div class="track">
              <div
                class="fill amq"
                style="width:${
                  (
                    m.amq /
                    maxVal *
                    100
                  ).toFixed(1)
                }%">
              </div>
            </div>

            <div class="num">
              ${m.amq.toFixed(1)}
            </div>

          </div>

        </div>
      `).join('');


  /* ==========================================================
     Simulation complete
     ========================================================== */

  document
    .getElementById('statusPill')
    .textContent = 'DONE';

  document
    .getElementById('statusPill')
    .className =
      'status-pill done';

  document
    .getElementById('nowBadge')
    .textContent =
      '— simulation complete —';

  document
    .getElementById('nowBadge')
    .className =
      'badge';
}


/* ============================================================
   Step simulation
   ============================================================ */

function stepSimulation() {

  if (!engine) {

    engine = startEngine();

    if (!engine) return;

    document
      .getElementById('statusPill')
      .textContent = 'RUNNING';

    document
      .getElementById('statusPill')
      .className =
        'status-pill running';
  }


  if (engine.done) {

    finalizeRun(engine);

    clearInterval(playTimer);
    playTimer = null;

    return;
  }


  const info =
    tickOnce(engine);

  paintTick(
    engine,
    info
  );


  if (
    engine.done ||
    engine.time > 500
  ) {

    finalizeRun(engine);

    clearInterval(playTimer);
    playTimer = null;
  }
}


document
  .getElementById('btnStep')
  .addEventListener(
    'click',
    stepSimulation
  );


/* ============================================================
   Run / Pause
   ============================================================ */

document
  .getElementById('btnRun')
  .addEventListener(
    'click',
    () => {

      if (playTimer) {

        clearInterval(playTimer);

        playTimer = null;

        document
          .getElementById('btnRun')
          .textContent =
            '▶ Run simulation';

        return;
      }


      if (!engine) {

        engine = startEngine();

        if (!engine) return;
      }


      document
        .getElementById('statusPill')
        .textContent =
          'RUNNING';

      document
        .getElementById('statusPill')
        .className =
          'status-pill running';


      document
        .getElementById('btnRun')
        .textContent =
          '⏸ Pause';


      const speed =
        +document
          .getElementById('speedRange')
          .value;


      const delay =
        Math.max(
          15,
          260 - speed * 24
        );


      playTimer =
        setInterval(
          () => {

            stepSimulation();

            if (
              !engine ||
              engine.done
            ) {

              clearInterval(
                playTimer
              );

              playTimer = null;

              document
                .getElementById('btnRun')
                .textContent =
                  '▶ Run simulation';
            }

          },
          delay
        );
    }
  );


/* ============================================================
   Initial hospital preset
   ============================================================ */

document
  .getElementById('btnPresetHospital')
  .click();