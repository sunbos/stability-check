// Mermaid 初始化：最简方案，多种触发方式兜底。
// 配套 mkdocs.yml 的 fence_div_format（输出 <div class="mermaid">）使用。
(function () {
  function tryInit() {
    if (typeof window.mermaid === "undefined") return false;
    try {
      window.mermaid.initialize({
        startOnLoad: false,
        theme: "default",
        securityLevel: "loose",
        flowchart: { useMaxWidth: true, htmlLabels: true },
      });
      // mermaid v10 的 run() 接受 { nodes: NodeList } 对象。
      // 早期传 { query: ".mermaid" } 触发 "Nodes and querySelector are both undefined" 错误。
      // 正确用法：手动 querySelectorAll 后传给 run({ nodes })。
      var nodes = document.querySelectorAll(".mermaid");
      if (nodes.length === 0) return true; // 没图也认为初始化成功
      var p = window.mermaid.run({ nodes: nodes });
      var onDone = function () {
        // mermaid 生成的 SVG 用 width="100%"，若 .mermaid 容器宽度坍缩为 0
        // （某些主题 CSS 把 .mermaid 当 inline 处理），SVG 也会变成 0x0。
        // 渲染后强制容器为 block + 100% 宽，SVG 用 auto 高度撑开。
        nodes.forEach(function (el) {
          el.style.display = "block";
          el.style.width = "100%";
          el.style.overflow = "visible";
          var svg = el.querySelector("svg");
          if (svg) {
            svg.style.display = "block";
            svg.style.maxWidth = "100%";
            svg.style.height = "auto";
          }
        });
      };
      if (p && typeof p.then === "function") {
        p.then(onDone).catch(function (e) {
          console.error("[mermaid-init] run failed:", e);
        });
      } else {
        // 同步路径：mermaid.run 已完成渲染
        onDone();
      }
      return true;
    } catch (e) {
      console.error("[mermaid-init] init failed:", e);
      return false;
    }
  }

  // 防重复初始化
  var done = false;
  function init() {
    if (done) return;
    if (tryInit()) done = true;
  }

  // 轮询兜底：mermaid.min.js 可能加载较慢
  var tries = 0;
  var h = setInterval(function () {
    tries++;
    if (typeof window.mermaid !== "undefined") {
      init();
      clearInterval(h);
    } else if (tries > 100) {
      clearInterval(h);
      console.error("[mermaid-init] mermaid.min.js not loaded after 5s");
    }
  }, 50);

  // DOMContentLoaded + window.load 兜底
  if (document.readyState !== "loading") {
    init();
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
  window.addEventListener("load", init);

  // Material SPA 切页后允许重新初始化
  if (window.document$) {
    window.document$.subscribe(function () {
      done = false;
      init();
    });
  }
})();
