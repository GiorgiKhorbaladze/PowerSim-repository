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
