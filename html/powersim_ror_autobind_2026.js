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
  const state={baseUrl:localStorage.getItem('powersim.applicationApi')||'http://localhost:8001',runId:null};
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
  window.PowerSimApplicationAPI={setBaseUrl,saveProject,createRun,launch,status,result,compare,projectFromLegacyPayload,submitCurrentUiProject,get state(){return {...state};}};
})();
