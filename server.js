/* ============================================================
 * 梗王冲浪挑战 · 联机单挑后端（零第三方依赖，Node 内置模块）
 * 运行：node server.js   （默认端口 8080）
 * 职责：房间管理 / 统一出题 / 作答校验 / 比分汇总 / 断线投降判负
 * ============================================================ */
const http=require('http');
const fs=require('fs');
const path=require('path');
const crypto=require('crypto');
const PORT=process.env.PORT||8080;
const ROOT=__dirname;

/* ---------- 题库 ---------- */
const QUESTIONS=JSON.parse(fs.readFileSync(path.join(ROOT,'questions.json'),'utf8'));
function shuffle(a){ a=a.slice(); for(let i=a.length-1;i>0;i--){ const j=Math.floor(Math.random()*(i+1)); [a[i],a[j]]=[a[j],a[i]]; } return a; }
function pick(arr,n){ if(arr.length<=n) return shuffle(arr); return shuffle(arr).slice(0,n); }
// 抽 8 题：easy3 + mid3 + hell2，服务端打乱选项并保留答案位置
function dealQuestions(){
  const easy=pick(QUESTIONS.filter(q=>q.diff==='easy'),3);
  const mid=pick(QUESTIONS.filter(q=>q.diff==='mid'),3);
  const hell=pick(QUESTIONS.filter(q=>q.diff==='hell'),2);
  return shuffle(easy.concat(mid,hell)).map(q=>{
    const order=shuffle([0,1,2,3]);
    return { q:q.q, options:order.map(i=>q.options[i]), ans:order.indexOf(q.ans), exp:q.exp, cat:q.cat };
  });
}

/* ---------- 房间表 ---------- */
const rooms=new Map(); // code -> room
function genCode(){
  let c; do{ c=String(100000+Math.floor(Math.random()*900000)); }while(rooms.has(c)); return c;
}
function newRoom(code){
  return { code, host:null, guest:null, status:'waiting', questions:[],
    score:{ host:{score:0,correct:0}, guest:{score:0,correct:0} },
    answered:{ host:new Set(), guest:new Set() }, finished:{host:false,guest:false} };
}

/* ---------- 极简 WebSocket 帧解析/组帧 ---------- */
const wsPeers=new Set();
let connSeq=0;
function acceptWebSocket(req, socket){
  const key=req.headers['sec-websocket-key'];
  const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  const peer={ socket, room:null, role:null, uid:null, name:'', buf:Buffer.alloc(0), connId:++connSeq, rport:socket.remotePort };
  wsPeers.add(peer);

  socket.on('data', chunk=>{
    peer.buf=Buffer.concat([peer.buf,chunk]);
    while(true){
      const frame=parseFrame(peer.buf);
      if(!frame) break;
      peer.buf=frame.rest;
      if(frame.opcode===8){ handleClose(peer); return; }       // close
      if(frame.opcode===1){ try{ onMessage(peer, JSON.parse(frame.payload.toString('utf8'))); }catch(e){} }
    }
  });
  socket.on('close', ()=>{ handleClose(peer); });
  socket.on('error', ()=>{ handleClose(peer); });
}
function parseFrame(buf){
  if(buf.length<2) return null;
  const b0=buf[0], b1=buf[1];
  const opcode=b0&0x0f; let len=b1&0x7f; let off=2;
  if(len===126){ if(buf.length<4) return null; len=buf.readUInt16BE(2); off=4; }
  else if(len===127){ if(buf.length<10) return null; len=Number(buf.readBigUInt64BE(2)); off=10; }
  const masked=(b1&0x80)===0x80;
  let maskStart=off;
  if(masked) off+=4;
  if(buf.length<off+len) return null;
  let payload=buf.slice(off,off+len);
  if(masked){ const mask=buf.slice(maskStart,maskStart+4); payload=Buffer.from(payload.map((x,i)=>x^mask[i%4])); }
  return { opcode, payload, rest:buf.slice(off+len) };
}
function send(peer, obj){
  if(!peer.socket||peer.socket.destroyed) return;
  const payload=Buffer.from(JSON.stringify(obj),'utf8');
  const len=payload.length; let header;
  if(len<126){ header=Buffer.from([0x81,len]); }
  else if(len<65536){ header=Buffer.alloc(4); header[0]=0x81; header[1]=126; header.writeUInt16BE(len,2); }
  else{ header=Buffer.alloc(10); header[0]=0x81; header[1]=127; header.writeBigUInt64BE(BigInt(len),2); }
  peer.socket.write(Buffer.concat([header,payload]));
}

/* ---------- 房间广播 ---------- */
function members(room){ return [room.host,room.guest].filter(Boolean); }
function broadcast(room, obj){ members(room).forEach(p=>send(p,obj)); }
function peerName(room,role){ return role==='host'?room.host.name:room.guest?room.guest.name:'等待好友…'; }

/* ---------- 业务消息 ---------- */
function onMessage(peer, msg){
  switch(msg.type){
    case 'hello': {
      peer.uid=msg.uid||crypto.randomUUID(); peer.name=(msg.name||'冲浪萌新').slice(0,6);
      send(peer,{type:'hello_ok', uid:peer.uid});
      break;
    }
    case 'create': {
      if(peer.room) leaveRoom(peer);
      const code=genCode(); const room=newRoom(code);
      room.host=peer; peer.room=room; peer.role='host';
      rooms.set(code,room);
      send(peer,{type:'created', code, role:'host', myName:peer.name});
      break;
    }
    case 'join': {
      const code=String(msg.code||'').trim();
      const room=rooms.get(code);
      if(!room){ send(peer,{type:'join_fail', reason:'房间不存在或邀请码错误'}); return; }
      if(room.host&&room.guest){ send(peer,{type:'join_fail', reason:'房间已满员'}); return; }
      if(peer.room) leaveRoom(peer);
      room.guest=peer; peer.room=room; peer.role='guest';
      send(peer,{type:'joined', role:'guest', code, myName:peer.name, foeName:room.host.name});
      send(room.host,{type:'guest_joined', guestName:peer.name});
      break;
    }
    case 'start': {
      const room=peer.room;
      if(!room||peer.role!=='host'||room.guest==null) return;
      room.questions=dealQuestions();
      room.status='playing';
      broadcast(room,{type:'start', questions:room.questions, hostName:room.host.name, guestName:room.guest.name});
      break;
    }
    case 'answer': {
      const room=peer.room; if(!room||room.status!=='playing') return;
      const idx=Number(msg.qIndex); const picked=Number(msg.picked);
      if(!Number.isInteger(idx)||!Number.isInteger(picked)) return;
      const q=room.questions[idx]; if(!q) return;
      if(room.answered[peer.role].has(idx)) return; // 每题只计一次
      room.answered[peer.role].add(idx);
      const correct=(picked===q.ans);
      const timeUsed=Math.max(1,Math.min(room.questions.length?room.questions.length*10:10, Math.round(msg.timeUsed||1)));
      let score=0;
      if(correct){ score=100+Math.max(0,50-Math.round(timeUsed/10*50)); room.score[peer.role].correct++; }
      room.score[peer.role].score+=score;
      // 把自己的对错与分数上报，同时同步双方比分
      broadcast(room,{type:'score', role:peer.role, correct, score,
        host:room.score.host, guest:room.score.guest, total:room.questions.length});
      // 双方都答完 → 结算
      const done=room.answered.host.size+room.answered.guest.size;
      if(done>=room.questions.length*2){ finishRoom(room,null); }
      break;
    }
    case 'leave': { handleClose(peer); break; }
  }
}

function finishRoom(room, loserRole){
  if(room.status==='finished') return;
  room.status='finished';
  let winner=null;
  if(loserRole){ winner=loserRole==='host'?'guest':'host'; }
  else{
    if(room.score.host.score>room.score.guest.score) winner='host';
    else if(room.score.host.score<room.score.guest.score) winner='guest';
    else winner='draw';
  }
  broadcast(room,{type:'finish', winner, host:room.score.host, guest:room.score.guest,
    hostName:room.host?room.host.name:'', guestName:room.guest?room.guest.name:''});
}

function leaveRoom(peer){
  const room=peer.room; if(!room) return;
  // 通知对手：对方离开
  const opponent=peer.role==='host'?room.guest:room.host;
  if(opponent){
    send(opponent,{type:'opponent_left'});
    if(room.status!=='finished') finishRoom(room, peer.role);
  }
  detach(peer,room);
}
function detach(peer,room){
  if(room.host===peer) room.host=null;
  if(room.guest===peer) room.guest=null;
  peer.room=null; peer.role=null;
  // 空房回收
  if(!room.host&&!room.guest) rooms.delete(room.code);
}
// 兜底：清理长时间无人且未开局的空房/僵尸房，避免房间表无限增长
setInterval(()=>{
  const now=Date.now();
  rooms.forEach((room,code)=>{
    if(room.status==='waiting'&&(!room.host||!room.guest)) rooms.delete(code);
  });
},60000);
function handleClose(peer){
  if(peer.room) leaveRoom(peer);
  wsPeers.delete(peer);
}

/* ---------- 静态文件服务 ---------- */
const MIME={ '.html':'text/html;charset=utf-8','.js':'text/javascript;charset=utf-8','.json':'application/json;charset=utf-8','.jpg':'image/jpeg','.jpeg':'image/jpeg','.png':'image/png','.css':'text/css','.svg':'image/svg+xml','.ico':'image/x-icon' };
const server=http.createServer((req,res)=>{
  let u=decodeURIComponent(req.url.split('?')[0]);
  if(u==='/'||u==='/index.html') u='/梗王冲浪挑战.html';
  let file=path.join(ROOT,u);
  fs.stat(file,(err,st)=>{
    if(err){ res.writeHead(404); res.end('404'); return; }
    if(st.isDirectory()) file=path.join(file,'梗王冲浪挑战.html');
    if(!file.startsWith(ROOT)){ res.writeHead(403); res.end(); return; }
    fs.readFile(file,(err2,data)=>{
      if(err2){ res.writeHead(404); res.end('404'); return; }
      const ext=path.extname(file).toLowerCase();
      const head={ 'Content-Type':MIME[ext]||'application/octet-stream', 'Content-Length':data.length };
      // 静态图片/资源给浏览器长缓存；HTML 不缓存，保证更新即时生效
      if(['.jpg','.jpeg','.png','.css','.js','.svg','.ico'].includes(ext)){ head['Cache-Control']='public, max-age=86400'; }
      else if(ext==='.html'){ head['Cache-Control']='no-cache'; }
      res.writeHead(200,head);
      res.end(data);
    });
  });
});
server.on('upgrade',(req,socket)=>{
  if(req.headers.upgrade&&req.headers.upgrade.toLowerCase()==='websocket'){
    acceptWebSocket(req,socket);
  }
});
server.listen(PORT,()=>{ console.log(`[梗王联机] 服务已启动: http://localhost:${PORT}`); });
