/* PowerSim application API connector - orchestration only, never solver physics. */
(function () {
  'use strict';
  const state={baseUrl:localStorage.getItem('powersim.applicationApi')||'http://localhost:8001',runId:null};
  async function request(path, method, body) {
    const response=await fetch(state.baseUrl.replace(/\/$/,'')+path,{method:method||'GET',headers:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body)});
    if(!response.ok) throw new Error(`Application API ${method||'GET'} ${path}: HTTP ${response.status}`);
    return response.json();
  }
  function setBaseUrl(url){ state.baseUrl=String(url).replace(/\/$/,''); localStorage.setItem('powersim.applicationApi',state.baseUrl); }
  async function saveProject(project){ return request('/projects','POST',project); }
  async function createRun(projectId,scenarioId){ const run=await request(`/projects/${encodeURIComponent(projectId)}/runs${scenarioId?`?scenario_id=${encodeURIComponent(scenarioId)}`:''}`,'POST',{}); state.runId=run.id; return run; }
  async function launch(runId){ return request(`/runs/${encodeURIComponent(runId||state.runId)}/launch`,'POST',{}); }
  async function status(runId){ return request(`/runs/${encodeURIComponent(runId||state.runId)}`); }
  async function result(runId){ return request(`/runs/${encodeURIComponent(runId||state.runId)}/result`); }
  async function compare(leftRunId,rightRunId){ return request(`/runs/compare?left_run_id=${encodeURIComponent(leftRunId)}&right_run_id=${encodeURIComponent(rightRunId)}`); }
  window.PowerSimApplicationAPI={setBaseUrl,saveProject,createRun,launch,status,result,compare,get state(){return {...state};}};
})();
