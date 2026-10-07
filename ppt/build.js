const pptxgen = require("pptxgenjs");
const React = require("react");
const RDS = require("react-dom/server");
const sharp = require("sharp");
const fa = require("react-icons/fa");
const { applyTheme } = require("/root/.claude/skills/synced/0eab9e73-749e-44e3-a44f-008a027f7511_9dbd74d6-19d7-4c17-88cc-c3e4613b7343/pptx/scripts/apply_theme.js");

const HEX = { navy:"1B1F3B", ink:"2A2F55", white:"FFFFFF", soft:"F3F4FA", coral:"FF6B5B", teal:"14B8A6", gray:"5B6075", lite:"C9CCE0" };
const THEME = { name:"AI Tools", headFontFace:"Arial", bodyFontFace:"Calibri",
  colors:{ dk1:HEX.navy, lt1:HEX.white, dk2:HEX.gray, lt2:HEX.soft, accent1:HEX.coral, accent2:HEX.teal,
    accent3:HEX.ink, accent4:"FFC857", accent5:"8B93FF", accent6:HEX.lite, hlink:HEX.teal, folHlink:HEX.ink } };

async function icon(Comp, color, size=256){
  const svg = RDS.renderToStaticMarkup(React.createElement(Comp,{color:"#"+color,size}));
  const buf = await sharp(Buffer.from(svg)).png().toBuffer();
  return "image/png;base64,"+buf.toString("base64");
}

(async()=>{
const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";
pres.theme = { headFontFace:THEME.headFontFace, bodyFontFace:THEME.bodyFontFace };
pres.title = "AI Tools: CiteGraph and LiveCrawl";
const C = pres.SchemeColor;

pres.defineSlideMaster({ title:"DARK", background:{color:HEX.navy},
  objects:[{placeholder:{options:{name:"title",type:"title",x:0.6,y:1.6,w:8.8,h:1.3,fontSize:40,bold:true,color:C.background1,valign:"bottom",align:"left"},text:""}},
           {placeholder:{options:{name:"body",type:"body",x:0.6,y:3.0,w:8.8,h:0.9,fontSize:18,color:HEX.lite,valign:"top"},text:""}}]});
pres.defineSlideMaster({ title:"CONTENT", background:{color:HEX.white},
  objects:[{placeholder:{options:{name:"title",type:"title",x:0.5,y:0.35,w:7.2,h:0.75,fontSize:30,bold:true,color:C.text1,valign:"middle",margin:0,align:"left"},text:""}}],
  slideNumber:{x:9.2,y:5.2,w:0.5,h:0.3,fontSize:10,color:HEX.gray}});

const I = {
  quote: await icon(fa.FaQuoteRight,HEX.coral), robot: await icon(fa.FaRobot,HEX.white), search: await icon(fa.FaSearch,HEX.white),
  bolt: await icon(fa.FaBolt,HEX.white), globe: await icon(fa.FaGlobe,HEX.teal), eye: await icon(fa.FaEye,HEX.white),
  wrench: await icon(fa.FaWrench,HEX.white), chart: await icon(fa.FaChartLine,HEX.white), redo: await icon(fa.FaRedo,HEX.white),
  save: await icon(fa.FaSave,HEX.white), news: await icon(fa.FaNewspaper,HEX.white), tag: await icon(fa.FaTag,HEX.white),
  trophy: await icon(fa.FaTrophy,HEX.white), users: await icon(fa.FaUsers,HEX.white), check: await icon(fa.FaCheck,HEX.white),
  clock: await icon(fa.FaClock,HEX.white), archive: await icon(fa.FaArchive,HEX.white), sync: await icon(fa.FaSyncAlt,HEX.white),
  file: await icon(fa.FaFilePdf,HEX.white), mic: await icon(fa.FaMicrophone,HEX.white), user: await icon(fa.FaUserTie,HEX.coral),
  shield: await icon(fa.FaShieldAlt,HEX.white), dollar: await icon(fa.FaDollarSign,HEX.white),
};

const T = (s,text,o)=>s.addText(text,Object.assign({isTextBox:true,fontFace:"Calibri",color:HEX.navy,margin:0},o));
const tag = (s,label,col)=>{ s.addShape(pres.shapes.ROUNDED_RECTANGLE,{x:7.9,y:0.45,w:1.6,h:0.45,fill:{color:col},rectRadius:0.22,line:{color:col}});
  T(s,label,{x:7.9,y:0.45,w:1.6,h:0.45,fontSize:12,bold:true,color:HEX.white,align:"center",valign:"middle"}); };
const circ = (s,x,y,d,col,img)=>{ s.addShape(pres.shapes.OVAL,{x,y,w:d,h:d,fill:{color:col},line:{color:col}});
  s.addImage({data:img,x:x+d*0.25,y:y+d*0.25,w:d*0.5,h:d*0.5}); };
const card = (s,x,y,w,h)=>s.addShape(pres.shapes.ROUNDED_RECTANGLE,{x,y,w,h,fill:{color:HEX.soft},line:{color:HEX.soft},rectRadius:0.12});
const content = (sec,title)=>{ const s=pres.addSlide({masterName:"CONTENT",sectionTitle:sec}); s.addText(title,{placeholder:"title"}); return s; };

// ---------- Intro ----------
pres.addSection({title:"Introduction"});
let s = pres.addSlide({masterName:"DARK",sectionTitle:"Introduction"});
s.addText("Two AI Tools, Explained Simply",{placeholder:"title"});
s.addText("CiteGraph  &  LiveCrawl: what they are, how they work, and why they matter",{placeholder:"body"});
circ(s,0.6,0.6,0.8,HEX.coral,I.robot);
s.addNotes("Welcome. Today we explain two AI tools in simple words: CiteGraph and LiveCrawl.");

s = content("Introduction","Today’s Agenda");
[[HEX.coral,I.quote,"CiteGraph","Helps your brand get mentioned BY AI chatbots","3 parts"],
 [HEX.teal,I.globe,"LiveCrawl","Helps AI read the LIVE web, not old copies","2 parts"]].forEach(([col,ic,name,desc,parts],i)=>{
  const x=0.5+i*4.6; card(s,x,1.4,4.3,3.4);
  s.addShape(pres.shapes.OVAL,{x:x+0.35,y:1.7,w:0.9,h:0.9,fill:{color:HEX.white},line:{color:col,width:2}});
  s.addImage({data:ic,x:x+0.55,y:1.9,w:0.5,h:0.5});
  T(s,name,{x:x+0.35,y:2.8,w:3.6,h:0.5,fontSize:24,bold:true,fontFace:"Arial",color:col});
  T(s,desc,{x:x+0.35,y:3.35,w:3.6,h:0.8,fontSize:16});
  T(s,parts,{x:x+0.35,y:4.2,w:3.6,h:0.35,fontSize:12,color:HEX.gray,italic:true});
});
s.addNotes("We cover CiteGraph first in three parts, then LiveCrawl in two parts, and finish with a quick comparison.");

// ---------- CiteGraph ----------
const CG="CiteGraph";
pres.addSection({title:CG});
s = pres.addSlide({masterName:"DARK",sectionTitle:CG});
s.addText("CiteGraph",{placeholder:"title"});
s.addText("Is AI recommending you, or your competitor?",{placeholder:"body"});
circ(s,0.6,0.6,0.8,HEX.coral,I.search);

// Part 1
s = content(CG,"What Is CiteGraph?"); tag(s,"Part 1 of 3",HEX.coral);
T(s,"A tool that checks if AI chatbots like ChatGPT, Claude, Gemini and Perplexity mention your product, and tells you how to get mentioned more.",{x:0.5,y:1.35,w:4.6,h:1.3,fontSize:17});
T(s,"Think of it as SEO, but for AI chatbots instead of Google.",{x:0.5,y:2.8,w:4.6,h:0.8,fontSize:16,italic:true,color:HEX.coral,bold:true});
T(s,"Also called GEO: Generative Engine Optimization",{x:0.5,y:3.7,w:4.6,h:0.4,fontSize:13,color:HEX.gray});
card(s,5.5,1.35,4.0,3.5);
T(s,"You ask ChatGPT:",{x:5.8,y:1.55,w:3.5,h:0.35,fontSize:13,color:HEX.gray});
T(s,"“What’s the best tool for X?”",{x:5.8,y:1.9,w:3.5,h:0.45,fontSize:16,bold:true});
s.addShape(pres.shapes.ROUNDED_RECTANGLE,{x:5.8,y:2.55,w:3.4,h:0.9,fill:{color:HEX.white},line:{color:HEX.lite},rectRadius:0.1});
T(s,"AI: “Try Competitor A or Competitor B.”",{x:5.95,y:2.55,w:3.1,h:0.9,fontSize:14,valign:"middle"});
T(s,"Your product isn’t mentioned, and you never find out.",{x:5.8,y:3.65,w:3.5,h:0.9,fontSize:14,bold:true,color:HEX.coral});
s.addNotes("CiteGraph checks whether AI chatbots recommend your product. It is like SEO, but for AI answers.");

s = content(CG,"Why Does It Matter?"); tag(s,"Part 1 of 3",HEX.coral);
[[I.users,"People changed habits","More people ask AI chatbots instead of searching Google"],
 [I.eye,"It’s invisible","You can’t see what AI says about you, so you can’t fix it"],
 [I.trophy,"Lost customers","If AI names a competitor, that buyer goes to them"]].forEach(([ic,h,d],i)=>{
  const x=0.5+i*3.1; card(s,x,1.4,2.8,3.3); circ(s,x+0.3,1.7,0.8,HEX.coral,ic);
  T(s,h,{x:x+0.3,y:2.7,w:2.3,h:0.5,fontSize:18,bold:true,fontFace:"Arial"});
  T(s,d,{x:x+0.3,y:3.2,w:2.3,h:1.3,fontSize:15,color:HEX.gray});
});
s.addNotes("Search habits are shifting to AI. Companies need to know if AI recommends them.");

// Part 2
s = content(CG,"How CiteGraph Works"); tag(s,"Part 2 of 3",HEX.coral);
[[I.mic,"Ask","Sends real buyer questions to AI chatbots"],[I.save,"Record","Saves every answer and which sites were used"],
 [I.chart,"Compare","Shows where competitors were named, not you"],[I.wrench,"Fix","Gives a simple to-do list to fix your pages"],
 [I.redo,"Re-check","Checks again every week to see progress"]].forEach(([ic,h,d],i)=>{
  const x=0.5+i*1.85; circ(s,x+0.45,1.5,0.85,i%2?HEX.navy:HEX.coral,ic);
  T(s,(i+1)+". "+h,{x,y:2.55,w:1.75,h:0.45,fontSize:17,bold:true,fontFace:"Arial",align:"center"});
  T(s,d,{x:x+0.05,y:3.05,w:1.65,h:1.3,fontSize:14,color:HEX.gray,align:"center"});
  if(i<4) s.addShape(pres.shapes.RIGHT_ARROW,{x:x+1.5,y:1.78,w:0.4,h:0.3,fill:{color:HEX.lite},line:{color:HEX.lite}});
});
T(s,"Simple idea: ask AI, see who wins, fix it, repeat.",{x:0.5,y:4.6,w:9,h:0.4,fontSize:15,italic:true,color:HEX.coral,bold:true});
s.addNotes("Five steps: ask, record, compare, fix, re-check weekly.");

s = content(CG,"A Simple Example"); tag(s,"Part 2 of 3",HEX.coral);
const ex=[["Before",HEX.gray,"Question: “Best payment API for startups?”","AI recommends 3 competitors. You are missing."],
          ["CiteGraph says",HEX.coral,"“Your docs don’t clearly say ‘for startups’.”","“Add a pricing comparison page.”"],
          ["After",HEX.teal,"You make the small fixes","Next week, AI starts naming you too"]];
ex.forEach(([h,col,a,b],i)=>{ const x=0.5+i*3.1; card(s,x,1.4,2.8,3.2);
  s.addShape(pres.shapes.ROUNDED_RECTANGLE,{x:x+0.25,y:1.6,w:2.3,h:0.5,fill:{color:col},line:{color:col},rectRadius:0.2});
  T(s,h,{x:x+0.25,y:1.6,w:2.3,h:0.5,fontSize:16,bold:true,color:HEX.white,align:"center",valign:"middle"});
  T(s,[{text:a,options:{breakLine:true,paraSpaceAfter:10}},{text:b}],{x:x+0.25,y:2.3,w:2.3,h:2.1,fontSize:15});
});
s.addNotes("Example: a payment company is missing from AI answers. CiteGraph points out simple fixes, and after the changes AI starts mentioning them.");

// Part 3
s = content(CG,"Benefits of CiteGraph"); tag(s,"Part 3 of 3",HEX.coral);
[[I.eye,"See the invisible","Know exactly what AI says about your brand"],
 [I.wrench,"Clear fixes","Gives a to-do list down to single sentences, not vague advice"],
 [I.robot,"4 AIs in one place","ChatGPT, Claude, Gemini and Perplexity"],
 [I.dollar,"Cheap to start","Free first scan, then about $12 per scan or a monthly plan"]].forEach(([ic,h,d],i)=>{
  const x=0.5+(i%2)*4.6, y=1.35+Math.floor(i/2)*1.75; card(s,x,y,4.4,1.5); circ(s,x+0.25,y+0.35,0.8,HEX.coral,ic);
  T(s,h,{x:x+1.3,y:y+0.2,w:2.9,h:0.45,fontSize:18,bold:true,fontFace:"Arial"});
  T(s,d,{x:x+1.3,y:y+0.65,w:2.9,h:0.75,fontSize:14,color:HEX.gray});
});
s.addNotes("Benefits: visibility, clear fixes, four AI engines, low cost to start.");

s = content(CG,"Who Uses CiteGraph?"); tag(s,"Part 3 of 3",HEX.coral);
[["Founders","Want to know whether AI recommends their startup"],["Marketing & SEO teams","Track brand visibility in AI answers"],
 ["API & developer-tool companies","Developers now ask AI which tool to use"]].forEach(([h,d],i)=>{
  const y=1.35+i*1.1; s.addImage({data:I.user,x:0.5,y:y+0.1,w:0.55,h:0.55});
  T(s,h,{x:1.3,y,w:3.9,h:0.4,fontSize:17,bold:true,fontFace:"Arial"}); T(s,d,{x:1.3,y:y+0.4,w:3.9,h:0.55,fontSize:14,color:HEX.gray}); });
s.addShape(pres.shapes.ROUNDED_RECTANGLE,{x:5.6,y:1.35,w:3.9,h:3.4,fill:{color:HEX.navy},line:{color:HEX.navy},rectRadius:0.12});
T(s,"Good to know",{x:5.9,y:1.55,w:3.3,h:0.4,fontSize:14,bold:true,color:HEX.coral});
T(s,"CiteGraph is a young, early-stage startup, so no big-name customers have been announced yet.",{x:5.9,y:2.0,w:3.3,h:1.2,fontSize:15,color:HEX.white});
T(s,"But the category, AI search visibility, is growing fast as people switch from Google to AI chatbots.",{x:5.9,y:3.2,w:3.3,h:1.3,fontSize:15,color:HEX.lite});
s.addNotes("Main users: founders, marketers, and API companies. CiteGraph is new, so no famous customers are public, but this space is growing quickly.");

// ---------- LiveCrawl ----------
const LC="LiveCrawl";
pres.addSection({title:LC});
s = pres.addSlide({masterName:"DARK",sectionTitle:LC});
s.addText("LiveCrawl (by Exa)",{placeholder:"title"});
s.addText("Giving AI the web as it is right now",{placeholder:"body"});
circ(s,0.6,0.6,0.8,HEX.teal,I.bolt);

// Part 1
s = content(LC,"What Is LiveCrawl?"); tag(s,"Part 1 of 2",HEX.teal);
T(s,"A feature of Exa, a search engine built for AI apps. It makes Exa visit a web page right now and return the latest version, instead of an old saved copy.",{x:0.5,y:1.35,w:4.6,h:1.6,fontSize:17});
T(s,"The problem: AI only knows what it learned in training, and saved copies of pages can be days old.",{x:0.5,y:3.1,w:4.6,h:1.0,fontSize:15,color:HEX.gray});
[[I.archive,HEX.gray,"Saved copy","Fast, but may be old"],[I.sync,HEX.teal,"LiveCrawl","Fresh, up-to-the-minute"]].forEach(([ic,col,h,d],i)=>{
  const y=1.35+i*1.75; card(s,5.5,y,4.0,1.5); circ(s,5.75,y+0.35,0.8,col,ic);
  T(s,h,{x:6.8,y:y+0.3,w:2.5,h:0.45,fontSize:18,bold:true,fontFace:"Arial",color:col===HEX.gray?HEX.navy:HEX.teal});
  T(s,d,{x:6.8,y:y+0.75,w:2.5,h:0.5,fontSize:15,color:HEX.gray}); });
s.addNotes("LiveCrawl is part of Exa. It fetches the live web page so AI gets fresh information.");

s = content(LC,"How LiveCrawl Works"); tag(s,"Part 1 of 2",HEX.teal);
[[I.robot,"AI app asks","“Get me this web page”"],[I.globe,"Exa visits live","Opens the real site right now"],
 [I.file,"Cleans it up","Turns pages and PDFs into plain text"],[I.check,"AI answers","Uses fresh, accurate facts"]].forEach(([ic,h,d],i)=>{
  const x=0.5+i*2.3; circ(s,x+0.55,1.4,0.9,HEX.teal,ic===I.globe?I.search:ic);
  T(s,(i+1)+". "+h,{x,y:2.45,w:2.0,h:0.45,fontSize:16,bold:true,fontFace:"Arial",align:"center"});
  T(s,d,{x,y:2.9,w:2.0,h:0.7,fontSize:14,color:HEX.gray,align:"center"});
  if(i<3) s.addShape(pres.shapes.RIGHT_ARROW,{x:x+1.75,y:1.7,w:0.5,h:0.3,fill:{color:HEX.lite},line:{color:HEX.lite}}); });
card(s,0.5,3.8,9.0,1.1);
T(s,[{text:"You choose the mode:  ",options:{bold:true}},{text:"Always fresh  ·  Fresh first, saved copy as backup  ·  Saved copy first  ·  Saved copy only"}],
  {x:0.75,y:3.8,w:8.5,h:0.6,fontSize:15,valign:"middle"});
T(s,"Trade-off: fresh = a bit slower, saved = faster",{x:0.75,y:4.35,w:8.5,h:0.4,fontSize:13,italic:true,color:HEX.teal});
s.addNotes("The app asks, Exa visits the site live, cleans the content, and the AI answers with fresh facts. You can pick between speed and freshness.");

// Part 2
s = content(LC,"Use Cases & Benefits"); tag(s,"Part 2 of 2",HEX.teal);
T(s,"Where it’s used",{x:0.5,y:1.3,w:4.3,h:0.4,fontSize:18,bold:true,fontFace:"Arial",color:HEX.teal});
[[I.news,"Company news & product launches"],[I.clock,"Live events: sports, conferences"],[I.tag,"Price tracking"],[I.chart,"Social media trends"]].forEach(([ic,t],i)=>{
  const y=1.85+i*0.75; circ(s,0.5,y,0.55,HEX.teal,ic); T(s,t,{x:1.25,y,w:3.6,h:0.55,fontSize:15,valign:"middle"}); });
card(s,5.2,1.3,4.3,3.6);
T(s,"Why it helps",{x:5.5,y:1.45,w:3.8,h:0.4,fontSize:18,bold:true,fontFace:"Arial",color:HEX.teal});
T(s,[{text:"Answers stay up to date",options:{bullet:true,breakLine:true}},{text:"Fewer wrong or made-up answers",options:{bullet:true,breakLine:true}},
  {text:"Clean text, ready for AI",options:{bullet:true,breakLine:true}},{text:"Handles tricky pages and PDFs",options:{bullet:true,breakLine:true}},
  {text:"You pick speed vs freshness",options:{bullet:true}}],{x:5.5,y:1.95,w:3.8,h:2.8,fontSize:15,paraSpaceAfter:8,margin:0.05});
s.addNotes("Used for news, events, prices and trends. Benefits: fresh, accurate, clean, flexible.");

s = content(LC,"Famous Companies Using Exa"); tag(s,"Part 2 of 2",HEX.teal);
["Cursor","Cognition","HubSpot","OpenRouter","Monday.com"].forEach((n,i)=>{
  const x=0.5+i*1.84; s.addShape(pres.shapes.ROUNDED_RECTANGLE,{x,y:1.35,w:1.7,h:0.8,fill:{color:HEX.soft},line:{color:HEX.lite},rectRadius:0.1});
  T(s,n,{x,y:1.35,w:1.7,h:0.8,fontSize:15,bold:true,align:"center",valign:"middle"}); });
[["$2.2B","Exa valuation (2026)"],["5,000+","companies use Exa"],["400K+","developers"]].forEach(([n,l],i)=>{
  const x=0.5+i*3.1; T(s,n,{x,y:2.6,w:2.8,h:1.0,fontSize:48,bold:true,fontFace:"Arial",color:HEX.teal});
  T(s,l,{x,y:3.6,w:2.8,h:0.4,fontSize:15,color:HEX.gray}); });
T(s,"Backed by top investors: Andreessen Horowitz (a16z) and Benchmark",{x:0.5,y:4.45,w:9,h:0.4,fontSize:14,italic:true});
s.addNotes("Exa is used by Cursor, Cognition, HubSpot, OpenRouter and Monday.com. It is valued at 2.2 billion dollars.");

// ---------- Wrap up ----------
pres.addSection({title:"Wrap-up"});
s = content("Wrap-up","Side by Side");
const rows=[["","CiteGraph","LiveCrawl"],["In one line","Gets your brand mentioned BY AI","Lets AI read the LIVE web"],
  ["Main users","Marketers, founders, API companies","Developers building AI apps"],["Key benefit","See and fix AI visibility","Fresh, accurate AI answers"],
  ["Famous users","Early-stage startup","Cursor, HubSpot, Monday.com"]];
s.addTable(rows.map((r,ri)=>r.map((c,ci)=>({text:c,options:{bold:ri===0||ci===0,fontSize:ri===0?17:15,
  color:ri===0?HEX.white:HEX.navy,fill:{color:ri===0?(ci===1?HEX.coral:ci===2?HEX.teal:HEX.navy):(ri%2?HEX.soft:HEX.white)},valign:"middle"}}))),
  {x:0.5,y:1.35,w:9,colW:[2,3.5,3.5],rowH:0.62,border:{type:"solid",pt:1,color:HEX.lite},fontFace:"Calibri"});
s.addNotes("Quick comparison: CiteGraph helps you get mentioned by AI, LiveCrawl helps AI read the live web.");

s = pres.addSlide({masterName:"DARK",sectionTitle:"Wrap-up"});
s.addText("Thank You",{placeholder:"title"});
s.addText("Questions?",{placeholder:"body"});
circ(s,0.6,0.6,0.8,HEX.teal,I.users);

await pres.writeFile({fileName:"AI_Tools_CiteGraph_LiveCrawl.pptx"});
await applyTheme("AI_Tools_CiteGraph_LiveCrawl.pptx",THEME);
})();
