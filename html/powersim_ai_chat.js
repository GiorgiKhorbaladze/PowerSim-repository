(function(){
  'use strict';
  const STORAGE_URL = 'powersim_ai_backend_url';
  const STORAGE_SESSION = 'powersim_ai_session_id';
  const DEFAULT_URL = 'http://localhost:8000';
  const MAX_JSON_CHARS = 18000;
  const state = {sessionId:null, backendUrl:DEFAULT_URL, online:false, busy:false};
  const $ = id => document.getElementById(id);
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

  function init(){
    state.sessionId = getSessionId();
    state.backendUrl = localStorage.getItem(STORAGE_URL) || DEFAULT_URL;
    if($('ai-backend-url')) $('ai-backend-url').value = state.backendUrl;
    addSystem('AI backend is not connected. Start the backend server to use chat.\n\nLocal setup:\n1. Install backend requirements\n2. Set OPENAI_API_KEY\n3. Run uvicorn backend.powersim_ai_server:app --reload --port 8000');
    health();
    setInterval(health, 30000);
    $('ai-chat-input')?.addEventListener('keydown', e=>{ if(e.key==='Enter' && (e.ctrlKey || e.metaKey)){ send(); } });
  }
  function getSessionId(){
    let sid = localStorage.getItem(STORAGE_SESSION);
    if(!sid){ sid = 'ps-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2,10); localStorage.setItem(STORAGE_SESSION,sid); }
    return sid;
  }
  function toggle(force){ const d=$('powersim-ai-chat'); if(d) d.classList.toggle('open', typeof force==='boolean'?force:!d.classList.contains('open')); }
  function saveBackendUrl(){ state.backendUrl = ($('ai-backend-url')?.value || DEFAULT_URL).replace(/\/$/,''); localStorage.setItem(STORAGE_URL,state.backendUrl); health(); }
  function setStatus(ok, text){ const el=$('ai-backend-status'); state.online=!!ok; if(el){ el.textContent=text || (ok?'online':'offline'); el.className='ai-status '+(ok?'ok':'bad'); } }
  function showError(msg){ const e=$('ai-error-panel'); if(!e) return; e.style.display=msg?'block':'none'; e.innerHTML=esc(msg); }
  async function api(path, payload){
    const res = await fetch(state.backendUrl + path, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload||{})});
    if(!res.ok) throw new Error(path+' failed: HTTP '+res.status);
    return res.json();
  }
  async function health(){
    saveUrlFromInputOnly();
    try{ const r = await fetch(state.backendUrl + '/api/ai/health', {cache:'no-store'}); setStatus(r.ok, r.ok?'online':'offline'); }
    catch(_e){ setStatus(false,'offline'); }
  }
  function saveUrlFromInputOnly(){ const v=$('ai-backend-url')?.value; if(v) state.backendUrl=v.replace(/\/$/,''); }
  function addMsg(role, text){ const box=$('ai-chat-messages'); if(!box) return; const div=document.createElement('div'); div.className='ai-msg '+role; div.innerHTML=esc(text); box.appendChild(div); box.scrollTop=box.scrollHeight; }
  function addSystem(text){ addMsg('system', text); }
  function currentLanguage(){ return $('ai-language')?.value || 'auto'; }
  function compact(obj){
    if(!obj) return null;
    const s = JSON.stringify(obj);
    if(s.length <= MAX_JSON_CHARS) return obj;
    return {truncated:true, original_chars:s.length, summary:sliceSummary(obj)};
  }
  function sliceSummary(obj){
    return {metadata:obj.metadata||null, system_summary:obj.system_summary||null, diagnostics:obj.diagnostics||null,
      hourly_system_sample:Array.isArray(obj.hourly_system)?obj.hourly_system.slice(0,24):undefined,
      assets:Array.isArray(obj.assets)?obj.assets.slice(0,50):undefined};
  }
  function buildAiContextSummary(){
    let input = null;
    try { if(typeof window.buildInputPayload === 'function') input = compact(window.buildInputPayload()); } catch(e){ input = {error:'input summary unavailable: '+e.message}; }
    const results = compact(window.STATE?.results || null);
    const compareSlots = (window.STATE?.compareSlots || []).map((s,i)=>({index:i, name:s.name, scenario:s.scenario, horizon:s.horizon, summary:sliceSummary(s.data||{})}));
    return {session_id:state.sessionId, language:currentLanguage(), current_input:input, current_results:results,
      ui_state:{active_tab:document.querySelector('.tab.on')?.dataset?.tab || null, compare_scenarios:compareSlots, assets_count:window.STATE?.assets?.length || 0}};
  }
  async function send(){
    const inp=$('ai-chat-input'); const message=(inp?.value||'').trim(); if(!message) return;
    showError(''); addMsg('user', message); inp.value=''; renderProgress('Thinking…');
    try{
      const context=buildAiContextSummary();
      const reply=await api('/api/ai/chat', {...context, message});
      renderProgress(''); addMsg('assistant', reply.reply || reply.message || reply.answer || 'No text reply returned.');
      renderActions(reply.proposed_actions || []);
      renderBackendResult(reply);
    }catch(e){ renderProgress(''); showError('AI backend is not connected. Start the backend server to use chat. '+e.message); }
  }
  function renderProgress(text){ let el=$('ai-progress'); const list=$('ai-action-list'); if(!el && list){ el=document.createElement('div'); el.id='ai-progress'; el.className='ai-progress'; list.prepend(el); } if(el) el.textContent=text||''; }
  function renderActions(actions){
    const list=$('ai-action-list'); if(!list) return; list.querySelectorAll('.ai-action').forEach(n=>n.remove());
    actions.forEach(a=>{ const card=document.createElement('div'); card.className='ai-action';
      const changes=Array.isArray(a.changes)?a.changes:Object.entries(a.changes||{}).map(([k,v])=>k+': '+JSON.stringify(v));
      card.innerHTML=`<h4>${esc(a.title||a.action_id||'Proposed action')}</h4><p>${esc(a.description||'')}</p><span class="ai-risk">${esc(a.risk_level||'review')}</span><ul class="ai-change-list">${changes.map(c=>`<li>${esc(c)}</li>`).join('')}</ul><div class="ai-result-actions"><button class="btn btn-p btn-s">Confirm</button><button class="btn btn-o btn-s">Cancel</button></div>`;
      card.querySelector('.btn-p').onclick=()=>confirmAction(a, card); card.querySelector('.btn-o').onclick=()=>card.remove(); list.appendChild(card); });
  }
  async function confirmAction(action, card){
    renderProgress('Running confirmed action…');
    try{ const res=await api('/api/ai/confirm', {session_id:state.sessionId, action_id:action.action_id, action, context:buildAiContextSummary()}); renderProgress(''); addMsg('assistant', res.reply || res.message || 'Confirmed action completed.'); renderBackendResult(res, card); }
    catch(e){ renderProgress(''); showError('Confirmed action failed: '+e.message); }
  }
  function renderBackendResult(res, card){
    if(!res) return; const host=card || $('ai-action-list'); if(!host) return; const wrap=document.createElement('div'); wrap.className='ai-result-actions';
    if(res.patched_input){ const b=button('Apply to current scenario',()=>applyPatchedInput(res.patched_input)); wrap.appendChild(b); }
    if(res.results_json || res.results){ const b=button('Import results',()=>importResultsObject(res.results_json || res.results)); wrap.appendChild(b); }
    if(res.comparison){ addMsg('assistant', 'Comparison summary:\n'+JSON.stringify(res.comparison,null,2)); }
    if(wrap.children.length) host.appendChild(wrap);
  }
  function button(text, fn){ const b=document.createElement('button'); b.className='btn btn-g btn-s'; b.type='button'; b.textContent=text; b.onclick=fn; return b; }
  function applyPatchedInput(input){ addMsg('system','Patched input received. Stage 1 does not overwrite all editor fields automatically; export/apply workflow hook is ready.'); window.PowerSimAIPatchedInput=input; }
  function importResultsObject(data){ if(!data) return; window.STATE.results=data; if(typeof window.renderWorkflowSteps==='function') window.renderWorkflowSteps(); if(typeof window.goTab==='function') window.goTab('results'); if(typeof window.renderResults==='function') window.renderResults(); if(typeof window.renderReports==='function') try{window.renderReports();}catch(e){} addMsg('system','Results imported into the Results tab.'); }
  function selfCheck(){
    return {hasDrawer:!!$('powersim-ai-chat'), hasBackendUrl:!!$('ai-backend-url'), noHardcodedSecret:!document.documentElement.innerHTML.match(new RegExp('s'+'k-[A-Za-z0-9_-]{20,}')), canBuildContext:!!buildAiContextSummary().session_id};
  }
  window.PowerSimAIChat = {init,toggle,send,saveBackendUrl,health,buildAiContextSummary,renderActions,importResultsObject,selfCheck};
  document.addEventListener('DOMContentLoaded', init);
})();
