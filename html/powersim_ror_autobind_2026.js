// PowerSim 2026 RoR profile auto-bind hook.
// Keeps the large GSE demo block conflict-free by applying profile seeding and
// hydro_ror dropdown rebinding after the base UI has loaded.
(function(){
  'use strict';

  const ROR_PROFILE_SCENARIO = 'MC_P50_base';
  const ROR_ZONE_BY_ASSET = {
    zahesi:'Z16_სამგორი', rionhesi:'Z03_რიონი-ალპანა', atshesi:'Z04_ტეხური',
    chitakhevhesi:'Z09_ფარავანი', ortachalhesi:'Z16_სამგორი',
    gumathesi_1:'Z03_რიონი-ალპანა', gumathesi_2:'Z03_რიონი-ალპანა',
    lajanurhesi:'Z03_რიონი-ალპანა',
    vartsikhehesi_1:'Z04_ტეხური', vartsikhehesi_2:'Z04_ტეხური',
    vartsikhehesi_3:'Z04_ტეხური', vartsikhehesi_4:'Z04_ტეხური',
    khadorhesi:'Z12_ალაზანი-ბირკიანი', larsihesi:'Z11_მთიულეთის არაგვი',
    paravanhesi:'Z09_ფარავანი', darialhesi:'Z11_მთიულეთის არაგვი',
    khelvachauri_1:'Z07_აჭარისწყალი', shuakhevihesi:'Z07_აჭარისწყალი',
    old_energy:'Z01_კოდორი', kirnatihesi:'Z07_აჭარისწყალი',
    mestiachala_2:'Z01_კოდორი', mestiachala_1:'Z01_კოდორი',
    khobihesi_2:'Z04_ტეხური', mtkvarihesi:'Z09_ფარავანი',
    satskhenhesi:'Z16_სამგორი', basra_1:'Z15_ჭოროხი', tetrikhevhesi:'Z16_სამგორი',
    bzhuzhahesi:'Z06_სუფსა', rachahesi:'Z02_რიონი-ონი', lakhami_2:'Z01_კოდორი',
    nakrahesi:'Z01_კოდორი', bakhvihesi_3:'Z06_სუფსა', akhmetahesi:'Z12_ალაზანი-ბირკიანი',
    sionhesi:'Z11_მთიულეთის არაგვი', kasletihesi_2:'Z01_კოდორი',
    skhaltahesi:'Z07_აჭარისწყალი', aragvihesi:'Z11_მთიულეთის არაგვი',
    sashuala_1:'Z06_სუფსა', akhalkalaki_1:'Z09_ფარავანი',
    small_hpp_remainder:'Z08_ლიახვი'
  };

  function bundleProfiles(){
    return (window.POWERSIM_ROR_PROFILES_2026 && window.POWERSIM_ROR_PROFILES_2026.profiles) || {};
  }

  function rorProfileKey(id, scenario = ROR_PROFILE_SCENARIO){
    const zone = ROR_ZONE_BY_ASSET[id];
    return zone ? `ror_${zone}_2026_${scenario}` : null;
  }

  function seedRoRProfiles(){
    if(!window.STATE) return;
    const profiles = bundleProfiles();
    const keys = Object.keys(profiles);
    if(!keys.length) return;
    window.STATE.profiles = window.STATE.profiles || {};
    Object.assign(window.STATE.profiles, profiles);
    window.STATE.profile_bundle = Object.assign({}, window.STATE.profile_bundle || {}, {
      hydro_source: 'PLEXOS_2026_zones_60files',
      hydro_inflow_unit: 'availability_factor',
      ror_profile_scenario: ROR_PROFILE_SCENARIO,
      ror_profile_count: keys.length
    });
  }

  function bindRoRProfilesToAssets(){
    if(!window.STATE || !Array.isArray(window.STATE.assets)) return;
    window.STATE.assets.forEach(a => {
      if(a.type !== 'hydro_ror') return;
      const pk = rorProfileKey(a.id);
      if(pk) a.availability_profile = pk;
    });
  }

  function patchRenderAssetTable(){
    if(typeof window.renderAssetTable !== 'function' || window.renderAssetTable.__rorProfilePatched) return;
    const original = window.renderAssetTable;
    window.renderAssetTable = function(...args){
      seedRoRProfiles();
      return original.apply(this, args);
    };
    window.renderAssetTable.__rorProfilePatched = true;
  }

  function patchLoadGSE2026Demo(){
    if(typeof window.loadGSE2026Demo !== 'function' || window.loadGSE2026Demo.__rorProfilePatched) return;
    const original = window.loadGSE2026Demo;
    window.loadGSE2026Demo = function(...args){
      const result = original.apply(this, args);
      seedRoRProfiles();
      bindRoRProfilesToAssets();
      if(typeof window.renderAssetTable === 'function') window.renderAssetTable();
      if(typeof window.renderProfileList === 'function') window.renderProfileList();
      if(typeof window.renderWorkflowSteps === 'function') window.renderWorkflowSteps();
      return result;
    };
    window.loadGSE2026Demo.__rorProfilePatched = true;
  }

  function install(){
    patchRenderAssetTable();
    patchLoadGSE2026Demo();
    seedRoRProfiles();
    bindRoRProfilesToAssets();
  }

  window.PowerSimRoRProfiles2026 = {
    scenario: ROR_PROFILE_SCENARIO,
    zoneByAsset: ROR_ZONE_BY_ASSET,
    rorProfileKey,
    seedRoRProfiles,
    bindRoRProfilesToAssets,
    install
  };
  install();
  document.addEventListener('DOMContentLoaded', install);
})();

// Application API connector. This is UI transport only - no solver physics.
(function(){
  'use strict';
  const sameOrigin=(window.location&&/^https?:$/.test(window.location.protocol))?`${window.location.origin}/api`:'http://localhost:8000/api';
  const state={baseUrl:localStorage.getItem('powersim.applicationApi')||sameOrigin,runId:null};
  async function request(path,method,body){
    const response=await fetch(state.baseUrl.replace(/\/$/,'')+path,{method:method||'GET',headers:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body)});
    if(!response.ok) throw new Error(`Application API ${method||'GET'} ${path}: HTTP ${response.status}`);
    return response.json();
  }
  function setBaseUrl(url){state.baseUrl=String(url).replace(/\/$/,'');localStorage.setItem('powersim.applicationApi',state.baseUrl);}
  async function saveProject(project){return request('/projects','POST',project);}
  async function createRun(projectId,scenarioId){const run=await request(`/projects/${encodeURIComponent(projectId)}/runs${scenarioId?`?scenario_id=${encodeURIComponent(scenarioId)}`:''}`,'POST',{});state.runId=run.id;return run;}
  async function launch(runId){return request(`/runs/${encodeURIComponent(runId||state.runId)}/launch`,'POST',{});}
  async function status(runId){return request(`/runs/${encodeURIComponent(runId||state.runId)}`);}
  async function result(runId){return request(`/runs/${encodeURIComponent(runId||state.runId)}/result`);}
  async function compare(leftRunId,rightRunId){return request(`/runs/compare?left_run_id=${encodeURIComponent(leftRunId)}&right_run_id=${encodeURIComponent(rightRunId)}`);}
  async function waitForCompletion(runId, timeoutMs){
    const deadline=Date.now()+(timeoutMs||300000);
    let latest;
    while(Date.now()<deadline){
      latest=await status(runId);
      setRunStatus(`run: ${latest.status}`,'info');
      if(latest.status==='completed') return latest;
      if(latest.status==='failed'||latest.status==='cancelled'){
        const event=(latest.events||[]).at(-1)||{};
        const detail=event.details?.error ? `: ${event.details.error}` : '';
        throw new Error(`Backend run ended with status ${latest.status}${detail}`);
      }
      await new Promise(resolve=>setTimeout(resolve,500));
    }
    throw new Error('Backend run did not complete before the UI timeout');
  }
  // Adapts the static editor's persisted input to the typed application API.
  // This carries data only; all validation and electrical equations remain in Python.
  function projectFromLegacyPayload(payload, projectId){
    if(!payload || typeof payload!=='object') throw new Error('A legacy input payload is required');
    const resolution=Number(payload.resolution_min||60);
    if(!Number.isFinite(resolution)||resolution<=0) throw new Error('resolution_min must be a positive number');
    const horizon=payload.study_horizon||{};
    const periods=Math.max(1,Math.round(Number(horizon.horizon_hours||24)*60/resolution));
    const metadata=payload.metadata||{};
    const year=Number(metadata.study_year||new Date().getUTCFullYear());
    const start=`${metadata.study_year||year}-01-01T00:00:00+04:00`;
    const profiles=Object.entries(payload.profiles||{}).map(([id,values])=>({id,unit:id==='demand'?'MW':'availability_factor',values:Array.isArray(values)?values:[]}));
    const assets=(payload.assets||[]).map(asset=>({
      id:String(asset.id||asset.name||''), kind:String(asset.type||'legacy_asset'), enabled:asset.enabled!==false,
      bus:asset.bus||asset.bus_id||null,
      profile_references:[asset.availability_profile,asset.inflow_profile].filter(Boolean),
      capacity_min_mw:Number.isFinite(Number(asset.pmin))?Number(asset.pmin):null,
      capacity_max_mw:Number.isFinite(Number(asset.pmax||asset.power_mw))?Number(asset.pmax||asset.power_mw):null,
      legacy_extensions:asset
    }));
    if(assets.some(asset=>!asset.id)) throw new Error('Every asset needs an id or name before submission');
    return {id:projectId||`ui-${Date.now()}`,version:{revision:1},metadata:{source:'PowerSim static UI',legacy_schema_version:metadata.schema_version||null},units:{currency:'USD'},time:{timezone:metadata.timezone||'Asia/Tbilisi',study_year:year,resolution_minutes:resolution,interval_duration_hours:resolution/60,start,periods,calendar_policy:'explicit_periods'},assets,profiles,reserve_products:payload.reserve_products||[],solver_settings:payload.solver_settings||{},legacy_payload:payload};
  }
  async function submitCurrentUiProject(projectId){
    if(typeof window.buildInputPayload!=='function') throw new Error('The PowerSim editor is not loaded');
    return saveProject(projectFromLegacyPayload(window.buildInputPayload(),projectId));
  }
  function showBackendResult(envelope){
    if(!envelope || !envelope.results) throw new Error('Backend returned no canonical result payload');
    if(envelope.validity!=='valid' || envelope.qa?.status!=='pass'){
      throw new Error(`Backend result is not publishable (validity=${envelope.validity||'unknown'}, QA=${envelope.qa?.status||'unknown'})`);
    }
    window.STATE.results=envelope.results;
    if(typeof window.renderWorkflowSteps==='function') window.renderWorkflowSteps();
    if(typeof window.goTab==='function') window.goTab('results');
    if(typeof window.renderResults==='function') window.renderResults();
    if(typeof window.renderReports==='function') try{window.renderReports();}catch(error){console.warn('Reports render skipped:',error);}
  }
  function setRunStatus(message,kind){
    const target=document.getElementById('powersim-backend-status');
    if(target){target.textContent=message;target.dataset.kind=kind||'info';}
  }
  async function runCurrentUiStudy(){
    const validation=typeof window.runValidation==='function'?window.runValidation():{errors:[]};
    if(validation.errors?.length) throw new Error('Correct UI validation errors before launching the backend');
    setRunStatus('პროექტი ინახება...','info');
    const saved=await submitCurrentUiProject();
    setRunStatus('run იქმნება...','info');
    const run=await createRun(saved.project_id);
    setRunStatus('solver მუშაობს...','info');
    const launched=await launch(run.id);
    if(!launched.accepted) throw new Error(launched.reason||'Backend did not accept the run');
    await waitForCompletion(run.id);
    const envelope=await result(run.id);
    showBackendResult(envelope);
    setRunStatus(`დასრულდა - QA: ${envelope.qa.status}; გამოქვეყნებადი: ${envelope.validity==='valid'?'დიახ':'არა'}`,'ok');
    return {run:launched.run,result:envelope};
  }
  function compactBackendDemo(projectId){
    const demand=Array.from({length:8760},()=>20);
    const legacyPayload={
      metadata:{study_year:2026,timezone:'Asia/Tbilisi',description:'PowerSim compact backend demonstration'},
      resolution_min:60,
      study_horizon:{start_hour:0,horizon_hours:4,mode:'full'},
      profiles:{demand},
      assets:[
        {id:'demo_thermal',name:'Demo thermal',type:'thermal',committable:true,pmin:0,pmax:30,
         heat_rate:0,fuel_price:0,vom:10,startup_cost:0,no_load_cost:0,ramp_up:30,ramp_down:30,
         min_up:0,min_down:0,initial_status:1,initial_power:20},
        {id:'demo_import',name:'Demo import',type:'import',pmax:30,vom:100}
      ],
      reserve_products:[],gas_constraints:{mode:'annual',unit:'Mm3',applies_to:[],annual:{cap:0}},
      solver_settings:{mip_gap:0.005,time_limit_s:30,rolling_window_h:168,rolling_step_h:24,unserved_penalty:3000,solver:'auto'}
    };
    return projectFromLegacyPayload(legacyPayload,projectId||`backend-demo-${Date.now()}`);
  }
  async function runCompactBackendDemo(){
    setRunStatus('სატესტო პროექტი ინახება...','info');
    const saved=await saveProject(compactBackendDemo());
    const run=await createRun(saved.project_id);
    setRunStatus('solver მუშაობს...','info');
    const launched=await launch(run.id);
    if(!launched.accepted) throw new Error(launched.reason||'Backend did not accept the run');
    await waitForCompletion(run.id);
    const envelope=await result(run.id);
    showBackendResult(envelope);
    setRunStatus(`სატესტო გამოთვლა დასრულდა - QA: ${envelope.qa.status}; გამოქვეყნებადი: ${envelope.validity==='valid'?'დიახ':'არა'}`,'ok');
    return {run:launched.run,result:envelope};
  }
  function installVisibleApplicationControls(){
    if(typeof document==='undefined'||typeof document.getElementById!=='function'||typeof document.querySelector!=='function') return;
    if(document.getElementById('powersim-backend-run')) return;
    const actions=document.querySelector('#pane-workflow .lpanel .bgrp');
    if(!actions) return;
    const button=document.createElement('button');
    button.id='powersim-backend-run'; button.className='btn btn-p'; button.type='button';
    button.textContent='▶ Backend გამოთვლა';
    button.addEventListener('click',async()=>{
      button.disabled=true;
      try{await runCurrentUiStudy();}
      catch(error){setRunStatus(`შეცდომა: ${error.message}`,'error'); if(typeof window.showBanner==='function') window.showBanner(`❌ Backend: ${error.message}`,'r',8000);}
      finally{button.disabled=false;}
    });
    const demo=document.createElement('button');
    demo.id='powersim-backend-demo'; demo.className='btn btn-b'; demo.type='button';
    demo.textContent='⚡ Backend Mini Demo';
    demo.addEventListener('click',async()=>{
      demo.disabled=true;
      try{await runCompactBackendDemo();}
      catch(error){setRunStatus(`შეცდომა: ${error.message}`,'error'); if(typeof window.showBanner==='function') window.showBanner(`❌ Backend: ${error.message}`,'r',8000);}
      finally{demo.disabled=false;}
    });
    const status=document.createElement('div');
    status.id='powersim-backend-status'; status.style.cssText='font-size:9px;color:var(--t2);line-height:1.4';
    status.textContent='ადგილობრივი API: მზად';
    actions.append(button,demo,status);
  }
  window.PowerSimApplicationAPI={setBaseUrl,saveProject,createRun,launch,status,result,compare,waitForCompletion,projectFromLegacyPayload,submitCurrentUiProject,runCurrentUiStudy,compactBackendDemo,runCompactBackendDemo,showBackendResult,installVisibleApplicationControls,get state(){return {...state};}};
  document.addEventListener('DOMContentLoaded',installVisibleApplicationControls);
  installVisibleApplicationControls();
})();
