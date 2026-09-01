  /* ===== Theme Management ===== */
  (function() {
    var stored = localStorage.getItem('mdpress-theme');
    var prefersDark = window.matchMedia('(prefers-color-scheme: dark)');

    function applyTheme(mode) {
      var wasDark = document.documentElement.classList.contains('dark');
      if (mode === 'dark' || (mode !== 'light' && prefersDark.matches)) {
        document.documentElement.classList.add('dark');
      } else {
        document.documentElement.classList.remove('dark');
      }
      var isDark = document.documentElement.classList.contains('dark');
      if (wasDark !== isDark) { reRenderMermaid(); }
    }

    function reRenderMermaid() {
      if (!window.mermaid) return;
      var nodes = document.querySelectorAll('.mermaid');
      if (!nodes.length) return;
      // Restore original source so mermaid can re-render from text
      for (var i = 0; i < nodes.length; i++) {
        var src = nodes[i].getAttribute('data-mermaid-src');
        if (src) { nodes[i].removeAttribute('data-processed'); nodes[i].textContent = src; }
      }
      try {
        window.mermaid.initialize({ startOnLoad: false, theme: document.documentElement.classList.contains('dark') ? 'dark' : 'default', securityLevel: 'strict', themeVariables: { fontFamily: '"PingFang SC","Hiragino Sans GB","Microsoft YaHei","Noto Sans SC","Noto Sans CJK SC","Source Han Sans SC",sans-serif' } });
        if (window.mermaid.run) { window.mermaid.run({ nodes: nodes }); }
        else if (window.mermaid.init) { window.mermaid.init(undefined, nodes); }
      } catch (e) { console.warn('[mdpress] Mermaid re-render failed', e); }
    }

    function setTheme(mode) {
      if (mode === 'system') {
        localStorage.removeItem('mdpress-theme');
      } else {
        localStorage.setItem('mdpress-theme', mode);
      }
      applyTheme(mode);
      updateToggleButtons(mode);
    }

    function updateToggleButtons(mode) {
      var buttons = document.querySelectorAll('.theme-toggle button');
      for (var i = 0; i < buttons.length; i++) {
        var btn = buttons[i];
        if (btn.getAttribute('data-theme') === mode) {
          btn.classList.add('active');
          btn.setAttribute('aria-pressed', 'true');
        } else {
          btn.classList.remove('active');
          btn.setAttribute('aria-pressed', 'false');
        }
      }
    }

    // Listen for system preference changes when in system mode
    prefersDark.addEventListener('change', function() {
      var current = localStorage.getItem('mdpress-theme');
      if (!current) applyTheme('system');
    });

    // Set up toggle buttons after DOM is ready
    document.addEventListener('DOMContentLoaded', function() {
      updateToggleButtons(stored || 'system');
      document.addEventListener('click', function(e) {
        var btn = e.target.closest('.theme-toggle button');
        if (btn) setTheme(btn.getAttribute('data-theme'));
      });
    });

    // Add permalink anchors to headings
    function addHeaderAnchors(root) {
      (root || document).querySelectorAll('.content h1[id], .content h2[id], .content h3[id], .content h4[id]').forEach(function(h) {
        if (h.querySelector('.header-anchor')) return;
        var a = document.createElement('a');
        a.className = 'header-anchor';
        a.href = '#' + h.id;
        a.textContent = '#';
        a.setAttribute('aria-hidden', 'true');
        h.prepend(a);
      });
    }
    addHeaderAnchors();

    window.__addHeaderAnchors = addHeaderAnchors;
    window.__setTheme = setTheme;
  })();

  var sidebar = document.querySelector('.sidebar');
  var body = document.body;
  var mainContent = document.querySelector('.content');
  var routeProgress = document.getElementById('route-progress');
  var sidebarToggle = document.querySelector('.sidebar-toggle');
  var prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var navUpdateFrame = null;
  var scrollSaveFrame = null;
  var lastActiveLink = null;
  var prefetchedPages = Object.create(null);
  var pageCache = Object.create(null);
  var pageCacheOrder = [];
  var pageCacheLimit = 50;
  var pendingNavigation = null;
  var internalNavStateKey = 'mdpress-site-nav';
  var scrollStoreKey = 'mdpress-site-scroll';
  var __page = window.__mdpressPage || {};
  var currentFile = __page.activeFile || '';
  /* index.html re-serves the first chapter, so sidebar highlighting has to
     match that chapter's entry while URL resolution keeps using index.html's
     own depth. navFile follows currentFile everywhere else. */
  var navFile = __page.navFile || currentFile;
  var navLinksByCurrentFile = [];
  var navChapterLinks = [];
  var navHeadingLinks = [];
  var headings = [];

  /* siteRoot is the absolute URL of the site root, derived from the current
     page location and the page's site-relative path. All generated hrefs are
     relative so the site works when served from a subdirectory (e.g. a
     GitHub Pages project site) and when opened via file://; the SPA router
     resolves everything against this root. */
  var siteRoot = (function() {
    try {
      var url = new URL('.', window.location.href);
      var segments = currentFile.split('/');
      for (var i = 0; i < segments.length - 1; i++) {
        url = new URL('..', url);
      }
      return url;
    } catch (e) {
      return null;
    }
  })();

  function resolveSiteHref(file) {
    if (!siteRoot) return file;
    try {
      return new URL(file, siteRoot).toString();
    } catch (e) {
      return file;
    }
  }

  /* The static sidebar and header links are emitted relative to this page.
     Rewrite them to absolute URLs once at load so they stay correct after
     SPA navigation changes the document URL (the sidebar HTML is not
     re-rendered during SPA page swaps). */
  (function() {
    var links = document.querySelectorAll('.sidebar a[href], .page-breadcrumb a[href]');
    for (var i = 0; i < links.length; i++) {
      var raw = links[i].getAttribute('href');
      if (!raw || raw.charAt(0) === '#' || /^[a-z][a-z0-9+.-]*:/i.test(raw)) continue;
      links[i].setAttribute('href', links[i].href);
    }
  })();

  try {
    window.sessionStorage.removeItem(internalNavStateKey);
  } catch (e) {}

  function getInternalPageURL(href) {
    if (!href) return null;
    try {
      var url = new URL(href, window.location.href);
      if (url.origin !== window.location.origin) return null;
      if (url.pathname === window.location.pathname) return null;
      if (!/\.html$/i.test(url.pathname)) return null;
      url.hash = '';
      return url.toString();
    } catch (e) {
      return null;
    }
  }

  function prefetchPage(href) {
    var pageURL = getInternalPageURL(href);
    if (!pageURL || prefetchedPages[pageURL]) return;
    prefetchedPages[pageURL] = true;

    var link = document.createElement('link');
    link.rel = 'prefetch';
    link.href = pageURL;
    link.as = 'document';
    document.head.appendChild(link);
  }

  function warmPageCache(href) {
    var pageURL = getInternalPageURL(href);
    if (!pageURL) return;
    var targetURL = new URL(pageURL, window.location.href);
    fetchPagePayload(targetURL).catch(function() {});
  }

  function rememberInternalNavigation(href) {
    var pageURL = getInternalPageURL(href);
    if (!pageURL) return;
    try {
      window.sessionStorage.setItem(internalNavStateKey, JSON.stringify({
        ts: Date.now(),
        href: pageURL
      }));
    } catch (e) {}
  }

  function getFileFromPathname(pathname) {
    if (!pathname) return currentFile || 'index.html';
    var clean = pathname;
    var rootPath = siteRoot && siteRoot.pathname ? siteRoot.pathname : '/';
    if (clean.indexOf(rootPath) === 0) {
      clean = clean.slice(rootPath.length);
    }
    clean = clean.replace(/\/+$/, '').replace(/^\/+/, '');
    try { clean = decodeURIComponent(clean); } catch (e) {}
    return clean || 'index.html';
  }

  function refreshPageContext() {
    var esc = CSS.escape ? CSS.escape(navFile) : navFile.replace(/[!"#$%&'()*+,.\/:;<=>?@\[\\\]^{|}~]/g, '\\$&');
    navLinksByCurrentFile = Array.from(document.querySelectorAll('.nav-item[data-file="' + esc + '"]'));
    navChapterLinks = Array.from(document.querySelectorAll('.nav-chapter[data-file="' + esc + '"]'));
    navHeadingLinks = Array.from(document.querySelectorAll('.nav-heading[data-file="' + esc + '"]'));
    headings = Array.from(document.querySelectorAll('.content h1[id], .content h2[id], .content h3[id], .content h4[id], .content h5[id], .content h6[id]'));
  }

  function setNavigating(isNavigating) {
    if (!mainContent) return;
    mainContent.classList.toggle('is-navigating', isNavigating);
  }

  function beginRouteProgress() {
    if (!routeProgress || prefersReducedMotion) return;
    routeProgress.classList.remove('is-finishing');
    routeProgress.classList.add('is-active');
  }

  function endRouteProgress() {
    if (!routeProgress || prefersReducedMotion) return;
    routeProgress.classList.remove('is-active');
    routeProgress.classList.add('is-finishing');
    window.setTimeout(function() {
      routeProgress.classList.remove('is-finishing');
    }, 220);
  }

  function readScrollStore() {
    try {
      return JSON.parse(window.sessionStorage.getItem(scrollStoreKey) || '{}');
    } catch (e) {
      return {};
    }
  }

  function writeScrollStore(store) {
    try {
      window.sessionStorage.setItem(scrollStoreKey, JSON.stringify(store));
    } catch (e) {}
  }

  function saveScrollPosition(pathname) {
    if (!pathname) return;
    var store = readScrollStore();
    store[pathname] = window.scrollY || window.pageYOffset || 0;
    writeScrollStore(store);
  }

  function getSavedScrollPosition(pathname) {
    if (!pathname) return null;
    var store = readScrollStore();
    return typeof store[pathname] === 'number' ? store[pathname] : null;
  }

  function expandGroupChain(group) {
    var current = group;
    while (current) {
      collapseSiblingGroups(current);
      setGroupExpanded(current, true);
      var parent = current.parentElement ? current.parentElement.closest('.nav-group') : null;
      if (!parent) break;
      current = parent;
    }
  }

  function setGroupExpanded(group, shouldExpand) {
    if (!group || !group.querySelector('.nav-children')) return;
    group.classList.toggle('collapsed', !shouldExpand);
    group.classList.toggle('expanded', shouldExpand);
    var toggle = group.querySelector(':scope > .nav-row > .nav-toggle');
    if (toggle) toggle.setAttribute('aria-expanded', shouldExpand ? 'true' : 'false');
  }

  function collapseSiblingGroups(group) {
    if (!group || !group.parentElement) return;
    var container = group.parentElement;
    var siblings = container.querySelectorAll(':scope > .nav-group');
    for (var i = 0; i < siblings.length; i++) {
      if (siblings[i] !== group && siblings[i].classList.contains('expanded')) {
        collapseGroupRecursive(siblings[i]);
      }
    }
  }

  function collapseGroupRecursive(group) {
    var childGroups = group.querySelectorAll('.nav-group.expanded');
    for (var i = 0; i < childGroups.length; i++) {
      setGroupExpanded(childGroups[i], false);
    }
    setGroupExpanded(group, false);
  }

  function toggleGroup(group) {
    if (!group) return;
    var shouldExpand = group.classList.contains('collapsed');
    if (shouldExpand) {
      collapseSiblingGroups(group);
    }
    setGroupExpanded(group, shouldExpand);
  }

  function smoothScrollToElement(element, hash) {
    if (!element) return;
    element.scrollIntoView({
      behavior: prefersReducedMotion ? 'auto' : 'smooth',
      block: 'start'
    });
    if (hash) {
      window.history.pushState(null, '', hash);
    }
  }

  function scrollToHashTarget(hash, shouldPushHistory) {
    if (!hash) {
      window.scrollTo({ top: 0, behavior: prefersReducedMotion ? 'auto' : 'smooth' });
      return;
    }

    var targetId = hash.charAt(0) === '#' ? hash.slice(1) : hash;
    var target = targetId ? document.getElementById(targetId) : null;
    if (!target) {
      if (shouldPushHistory) {
        window.history.pushState(null, '', '#' + targetId);
      }
      return;
    }

    target.scrollIntoView({
      behavior: prefersReducedMotion ? 'auto' : 'smooth',
      block: 'start'
    });
    if (shouldPushHistory) {
      window.history.pushState(null, '', '#' + targetId);
    }
  }

  function keepActiveLinkVisible(link) {
    if (!link || !sidebar) return;
    if (lastActiveLink === link) return;
    lastActiveLink = link;
    link.scrollIntoView({
      block: 'nearest',
      behavior: prefersReducedMotion ? 'auto' : 'smooth'
    });
  }

  function scrollToTopImmediate() {
    // Temporarily disable CSS scroll-behavior:smooth so the jump is instant.
    var root = document.documentElement;
    root.style.scrollBehavior = 'auto';
    window.scrollTo({ top: 0, left: 0, behavior: 'auto' });
    root.scrollTop = 0;
    document.body.scrollTop = 0;
    // Restore after the current frame so smooth scrolling works for user interactions.
    requestAnimationFrame(function() { root.style.scrollBehavior = ''; });
  }

  document.querySelectorAll('.nav-group').forEach(function(group) {
    var toggle = group.querySelector('.nav-toggle');
    var chapterLink = group.querySelector('.nav-chapter[data-group-link="true"]');

    if (toggle) {
      toggle.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        toggleGroup(group);
      });
    }

    if (chapterLink) {
      chapterLink.addEventListener('pointerenter', function() {
        prefetchPage(chapterLink.href);
      }, { passive: true });
      chapterLink.addEventListener('focus', function() {
        prefetchPage(chapterLink.href);
      });
      chapterLink.addEventListener('touchstart', function() {
        prefetchPage(chapterLink.href);
      }, { passive: true });
      chapterLink.addEventListener('click', function(e) {
        if (group.classList.contains('expanded')) {
          toggleGroup(group);
        } else {
          expandGroupChain(group);
        }
        rememberInternalNavigation(chapterLink.href);
        if (chapterLink.getAttribute('data-file') === currentFile) {
          e.preventDefault();
          scrollToTopImmediate();
        }
      });
    }
  });

  // --- Heading tracking via IntersectionObserver ---
  var headingObserver = null;
  var headingScrollHandler = null;
  var visibleHeadings = Object.create(null); // id -> true/false

  function activateNavForHeading(headingId) {
    document.querySelectorAll('.nav-item.active').forEach(function(link) {
      link.classList.remove('active');
    });

    var matched = false;
    if (headingId) {
      for (var j = 0; j < navHeadingLinks.length; j++) {
        if (navHeadingLinks[j].getAttribute('data-target') === headingId) {
          navHeadingLinks[j].classList.add('active');
          var activeGroup = navHeadingLinks[j].closest('.nav-group');
          expandGroupChain(activeGroup);
          keepActiveLinkVisible(navHeadingLinks[j]);
          matched = true;
          break;
        }
      }
    }

    if (!matched && navChapterLinks.length > 0) {
      navChapterLinks[0].classList.add('active');
      var activeChapterGroup = navChapterLinks[0].closest('.nav-group');
      expandGroupChain(activeChapterGroup);
      keepActiveLinkVisible(navChapterLinks[0]);
    }
  }

  function pickActiveHeading() {
    // Among headings in or above the viewport, pick the last one that is visible
    // (i.e. the deepest one the user has scrolled past).
    for (var i = headings.length - 1; i >= 0; i--) {
      if (visibleHeadings[headings[i].id]) {
        return headings[i].id;
      }
    }
    // Fallback: find topmost heading above viewport
    for (var k = headings.length - 1; k >= 0; k--) {
      if (headings[k].getBoundingClientRect().top <= 140) {
        return headings[k].id;
      }
    }
    return null;
  }

  function setupHeadingObserver() {
    if (headingObserver) { headingObserver.disconnect(); }
    if (headingScrollHandler) { window.removeEventListener('scroll', headingScrollHandler); headingScrollHandler = null; }
    visibleHeadings = Object.create(null);

    if (typeof IntersectionObserver === 'undefined') {
      // Fallback for older browsers: use scroll event.
      headingScrollHandler = function() { activateNavForHeading(pickActiveHeading()); };
      window.addEventListener('scroll', headingScrollHandler, { passive: true });
      return;
    }

    headingObserver = new IntersectionObserver(function(entries) {
      entries.forEach(function(entry) {
        visibleHeadings[entry.target.id] = entry.isIntersecting;
      });
      activateNavForHeading(pickActiveHeading());
    }, { rootMargin: '-80px 0px -60% 0px', threshold: 0 });

    headings.forEach(function(h) { headingObserver.observe(h); });
  }

  function updateActiveNavigation() {
    activateNavForHeading(pickActiveHeading());
  }

  function syncSidebarForCurrentFile() {
    document.querySelectorAll('.nav-group[data-group-file]').forEach(function(group) {
      if (group.getAttribute('data-group-file') === navFile) {
        expandGroupChain(group);
      }
    });
  }

  // --- Right-side page TOC ---
  var pageToc = document.getElementById('page-toc');
  var pageTocNav = document.getElementById('page-toc-nav');
  var mainBody = document.querySelector('.main-body');
  var tocObserver = null;
  var tocVisibleMap = Object.create(null);

  // Single click handler via delegation (never re-attached).
  if (pageTocNav) {
    pageTocNav.addEventListener('click', function(e) {
      var link = e.target.closest('a[data-toc-target]');
      if (!link) return;
      e.preventDefault();
      var target = document.getElementById(link.getAttribute('data-toc-target'));
      if (target) {
        target.scrollIntoView({ behavior: prefersReducedMotion ? 'auto' : 'smooth', block: 'start' });
        window.history.pushState(null, '', '#' + link.getAttribute('data-toc-target'));
      }
    });
  }

  function headingTextForTOC(heading) {
    if (!heading) return '';
    var clone = heading.cloneNode(true);
    clone.querySelectorAll('.header-anchor').forEach(function(anchor) {
      anchor.remove();
    });
    var text = (clone.textContent || '').trim();
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function buildPageTOC() {
    if (!pageTocNav || !pageToc) return;
    var tocHeadings = Array.from(document.querySelectorAll('.content h2[id], .content h3[id], .content h4[id]'));
    if (tocHeadings.length === 0) {
      // Collapse the TOC rail. The .toc-collapsed class on .main-body frees
      // the grid column in browsers without :has() support.
      pageToc.classList.add('toc-hidden');
      if (mainBody) mainBody.classList.add('toc-collapsed');
      pageTocNav.innerHTML = '';
      return;
    }
    pageToc.classList.remove('toc-hidden');
    if (mainBody) mainBody.classList.remove('toc-collapsed');
    var html = '';
    for (var i = 0; i < tocHeadings.length; i++) {
      var h = tocHeadings[i];
      var tag = h.tagName.toLowerCase();
      var depthClass = tag === 'h3' ? ' toc-depth-2' : tag === 'h4' ? ' toc-depth-3' : '';
      var safeId = h.id.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      html += '<a href="#' + safeId + '" data-toc-target="' + safeId + '" class="toc-link' + depthClass + '">' + headingTextForTOC(h) + '</a>';
    }
    pageTocNav.innerHTML = html;
    setupTocObserver(tocHeadings);
  }

  function setupTocObserver(tocHeadings) {
    if (tocObserver) tocObserver.disconnect();
    tocVisibleMap = Object.create(null);

    if (typeof IntersectionObserver === 'undefined' || !pageTocNav) return;

    tocObserver = new IntersectionObserver(function(entries) {
      entries.forEach(function(entry) {
        tocVisibleMap[entry.target.id] = entry.isIntersecting;
      });
      // Pick the topmost visible heading
      var activeId = null;
      for (var i = 0; i < tocHeadings.length; i++) {
        if (tocVisibleMap[tocHeadings[i].id]) { activeId = tocHeadings[i].id; break; }
      }
      if (!activeId) {
        // Fallback: find topmost heading above viewport
        for (var k = tocHeadings.length - 1; k >= 0; k--) {
          if (tocHeadings[k].getBoundingClientRect().top <= 140) { activeId = tocHeadings[k].id; break; }
        }
      }
      var links = pageTocNav.querySelectorAll('.toc-link');
      for (var j = 0; j < links.length; j++) {
        links[j].classList.toggle('toc-active', links[j].getAttribute('data-toc-target') === activeId);
      }
    }, { rootMargin: '-80px 0px -60% 0px', threshold: 0 });

    tocHeadings.forEach(function(h) { tocObserver.observe(h); });
  }

  var resizeTimer = null;
  function scheduleNavigationUpdate() {
    if (navUpdateFrame !== null) return;
    navUpdateFrame = window.requestAnimationFrame(function() {
      updateActiveNavigation();
      navUpdateFrame = null;
    });
  }

  window.addEventListener('resize', function() {
    if (resizeTimer) clearTimeout(resizeTimer);
    resizeTimer = setTimeout(scheduleNavigationUpdate, 200);
  });
  window.addEventListener('hashchange', function() {
    scheduleNavigationUpdate();
  });

  // markAssetFailure puts a visible notice on the page when a third-party
  // library could not be loaded. Offline, air-gapped and strict-CSP readers
  // used to get a silently blank area and a console warning they never saw;
  // now the page says what is missing, and the diagram source stays legible
  // instead of being laid out as if it were a rendered SVG.
  //
  // perNode is true for block elements such as diagrams, which are far apart
  // and each deserve their own notice; it is false for inline math, where one
  // banner per formula would shred the prose it sits in.
  function markAssetFailure(selector, message, perNode) {
    function notice() {
      var note = document.createElement('span');
      note.className = 'asset-error';
      note.setAttribute('role', 'status');
      note.textContent = message;
      return note;
    }
    var nodes = document.querySelectorAll(selector);
    var flagged = [];
    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i];
      if (node.getAttribute('data-mdpress-asset-error') === 'true') continue;
      node.setAttribute('data-mdpress-asset-error', 'true');
      node.setAttribute('title', message);
      flagged.push(node);
    }
    if (!flagged.length) return;
    if (perNode) {
      for (var j = 0; j < flagged.length; j++) {
        flagged[j].parentNode.insertBefore(notice(), flagged[j]);
      }
      return;
    }
    var host = document.querySelector('.content') || document.body;
    host.insertBefore(notice(), host.firstChild);
  }

  // loadCDNScript is a helper to load a script from CDN only once.
  // tag: data attribute name used to deduplicate; src: CDN URL;
  // integrity: Subresource Integrity digest; onReady: callback;
  // onFail: callback invoked when the script cannot be loaded or fails the
  // integrity check.
  function loadCDNScript(tag, src, integrity, onReady, onFail) {
    var attrName = 'data-mdpress-' + tag.replace(/[A-Z]/g, function(ch) {
      return '-' + ch.toLowerCase();
    });
    function fail() {
      console.warn('mdpress: CDN script failed to load: ' + src);
      if (onFail) onFail();
    }
    var existing = document.querySelector('script[' + attrName + ']');
    if (existing) {
      if (existing.dataset.mdpressLoaded === 'true') {
        if (onReady) onReady();
      } else if (existing.dataset.mdpressFailed === 'true') {
        fail();
      } else {
        if (onReady) existing.addEventListener('load', onReady, { once: true });
        existing.addEventListener('error', fail, { once: true });
      }
      return;
    }

    var s = document.createElement('script');
    s.src = src;
    if (integrity) s.integrity = integrity;
    s.crossOrigin = 'anonymous';
    s.referrerPolicy = 'no-referrer';
    s.setAttribute(attrName, 'true');
    s.addEventListener('load', function() {
      s.dataset.mdpressLoaded = 'true';
      if (onReady) onReady();
    }, { once: true });
    s.addEventListener('error', function() {
      s.dataset.mdpressFailed = 'true';
      fail();
    }, { once: true });
    document.body.appendChild(s);
  }

  function getMermaidTheme() {
    return document.documentElement.classList.contains('dark') ? 'dark' : 'default';
  }

  function ensureMermaid() {
    var nodes = document.querySelectorAll('.mermaid');
    if (!nodes.length) return;

    function runMermaid() {
      if (!window.mermaid) return;
      try {
        // Save original source for re-rendering on theme toggle
        for (var j = 0; j < nodes.length; j++) {
          if (!nodes[j].getAttribute('data-mermaid-src')) {
            nodes[j].setAttribute('data-mermaid-src', nodes[j].textContent);
          }
        }
        window.mermaid.initialize({ startOnLoad: true, theme: getMermaidTheme(), securityLevel: 'strict', themeVariables: { fontFamily: '"PingFang SC","Hiragino Sans GB","Microsoft YaHei","Noto Sans SC","Noto Sans CJK SC","Source Han Sans SC",sans-serif' } });
        if (window.mermaid.run) {
          window.mermaid.run({ nodes: nodes });
        } else if (window.mermaid.init) {
          window.mermaid.init(undefined, nodes);
        }
      } catch (e) {
        console.warn('[mdpress] Mermaid re-init failed', e);
      }
    }

    if (window.mermaid) { runMermaid(); return; }
    loadCDNScript('mermaid', 'https://cdn.jsdelivr.net/npm/mermaid@11.16.0/dist/mermaid.min.js', 'sha384-T/0lMUdJpd2S1ZHtRiofG3htU3xPCrFVeAQ1UUE2TJwlEJSV5NUwn30kP28n238E', runMermaid, function() {
      markAssetFailure('.mermaid', __ui.assetsMermaidFailed, true);
    });
  }

  // ensureKaTeX loads KaTeX and triggers auto-render when math elements are found.
  // Called on initial load and after each client-side navigation.
  function ensureKaTeX() {
    if (!document.querySelector('.math')) return;

    function runKaTeX() {
      if (typeof renderMathInElement !== 'function') return;
      try {
        var katexRoot = document.querySelector('.content') || document.body;
        renderMathInElement(katexRoot, {
          delimiters: [
            {left: '$$', right: '$$', display: true},
            {left: '$',  right: '$',  display: false}
          ],
          throwOnError: false
        });
      } catch (e) {
        console.warn('[mdpress] KaTeX render failed', e);
      }
    }

    if (typeof renderMathInElement === 'function') { runKaTeX(); return; }

    function mathFailed() {
      markAssetFailure('.math', __ui.assetsKatexFailed, false);
    }

    // Load KaTeX CSS if not already loaded.
    if (!document.querySelector('link[data-mdpress-katex-css]')) {
      var link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = 'https://cdn.jsdelivr.net/npm/katex@0.16.44/dist/katex.min.css';
      link.integrity = 'sha384-irXK0JiCGinqGL+slwVklbhJetrjczNwaP2lANewD8lKAs9n61SbQ3As28iSqXUE';
      link.crossOrigin = 'anonymous';
      link.referrerPolicy = 'no-referrer';
      link.dataset.mdpressKatexCss = 'true';
      link.addEventListener('error', mathFailed, { once: true });
      document.head.appendChild(link);
    }

    loadCDNScript('katex', 'https://cdn.jsdelivr.net/npm/katex@0.16.44/dist/katex.min.js', 'sha384-m/s9umSlhJbqEdA/j7pQVdGCMx2fHf7GXtgCVhNGOwLuu+1qJQES5AzIE8pn3nKQ', function() {
      loadCDNScript('katexAutoRender', 'https://cdn.jsdelivr.net/npm/katex@0.16.44/dist/contrib/auto-render.min.js', 'sha384-bjyGPfbij8/NDKJhSGZNP/khQVgtHUE5exjm4Ydllo42FwIgYsdLO2lXGmRBf5Mz', runKaTeX, mathFailed);
    }, mathFailed);
  }

  function getClientNavigation(anchor) {
    if (!anchor || !anchor.href) return null;
    try {
      var url = new URL(anchor.href, window.location.href);
      if (url.origin !== window.location.origin) return null;
      if (!/\.html$/i.test(url.pathname) && url.pathname !== window.location.pathname) return null;
      return {
        url: url,
        file: getFileFromPathname(url.pathname),
        hash: url.hash || ''
      };
    } catch (e) {
      return null;
    }
  }

  function parseFetchedPage(html, fallbackURL) {
    var doc = new DOMParser().parseFromString(html, 'text/html');
    var content = doc.querySelector('.content');
    if (!content) return null;
    var breadcrumbNav = doc.querySelector('.page-breadcrumb');
    var breadcrumbHTML = breadcrumbNav ? breadcrumbNav.innerHTML : '';
    return {
      title: doc.title || document.title,
      contentHTML: content.innerHTML,
      breadcrumbHTML: breadcrumbHTML,
      url: fallbackURL
    };
  }

  function getCachedPage(cacheKey) {
    var entry = pageCache[cacheKey];
    if (!entry) return null;
    // Move to end of access order (most recently used).
    var idx = pageCacheOrder.indexOf(cacheKey);
    if (idx !== -1) {
      pageCacheOrder.splice(idx, 1);
    }
    pageCacheOrder.push(cacheKey);
    return entry;
  }

  function cachePage(cacheKey, payload) {
    if (!pageCache[cacheKey]) {
      // New entry: evict the least recently used if over limit.
      if (pageCacheOrder.length >= pageCacheLimit) {
        var evict = pageCacheOrder.shift();
        delete pageCache[evict];
      }
      pageCacheOrder.push(cacheKey);
    } else {
      // Existing entry: refresh access order.
      var idx = pageCacheOrder.indexOf(cacheKey);
      if (idx !== -1) {
        pageCacheOrder.splice(idx, 1);
      }
      pageCacheOrder.push(cacheKey);
    }
    pageCache[cacheKey] = payload;
    return payload;
  }

  function fetchPagePayload(targetURL, signal) {
    var cacheKey = targetURL.origin + targetURL.pathname;
    var cached = getCachedPage(cacheKey);
    if (cached) return Promise.resolve(cached);

    return fetch(cacheKey, {
      credentials: 'same-origin',
      signal: signal
    }).then(function(response) {
      if (!response.ok) throw new Error('HTTP ' + response.status);
      return response.text().then(function(html) {
        var responseURL = new URL(response.url || cacheKey, window.location.href);
        var payload = parseFetchedPage(html, responseURL);
        if (!payload) throw new Error('Missing .content in fetched page');
        return cachePage(cacheKey, payload);
      });
    });
  }

  function finalizeNavigation(targetURL, options) {
    currentFile = getFileFromPathname(targetURL.pathname);
    navFile = currentFile;
    refreshPageContext();
    syncSidebarForCurrentFile();
    setupHeadingObserver();
    // buildPageTOC + addCopyButtons are called inside applySwap() so they
    // run before the new frame is painted (inside startViewTransition).
    updateActiveNavigation();
    ensureMermaid();
    ensureKaTeX();

    if (window.innerWidth <= 768) {
      sidebar.classList.remove('open');
    }

    if (options.updateHistory === 'push') {
      window.history.pushState({ path: targetURL.pathname }, '', targetURL.pathname + targetURL.search + targetURL.hash);
    } else if (options.updateHistory === 'replace') {
      window.history.replaceState({ path: targetURL.pathname }, '', targetURL.pathname + targetURL.search + targetURL.hash);
    }

    if (options.hash) {
      scrollToHashTarget(options.hash, false);
    } else if (options.restoreScroll === true) {
      var savedScroll = getSavedScrollPosition(targetURL.pathname);
      window.scrollTo({
        top: savedScroll || 0,
        behavior: prefersReducedMotion ? 'auto' : 'smooth'
      });
    } else if (options.scrollToTop !== false) {
      scrollToTopImmediate();
    }

    if (typeof window.__saveRecentPage === 'function') {
      window.__saveRecentPage();
    }
    if (typeof window.__showSearchJumpNotice === 'function') {
      window.__showSearchJumpNotice();
    }
    if (typeof window.__mdpressRefreshServePanel === 'function') {
      window.__mdpressRefreshServePanel();
    }
  }

  function swapPageContent(payload, targetURL, options) {
    function applySwap() {
      mainContent.innerHTML = payload.contentHTML;
      document.title = payload.title;
      var breadcrumbNav = document.querySelector('.page-breadcrumb');
      if (breadcrumbNav && payload.breadcrumbHTML) {
        breadcrumbNav.innerHTML = payload.breadcrumbHTML;
      }
      // Mutate DOM while the old snapshot is still displayed (inside the
      // view-transition callback) so wrapping <pre> and building the TOC
      // never cause a visible layout shift.
      if (window.__addCopyButtons) window.__addCopyButtons(mainContent);
      if (window.__addHeaderAnchors) window.__addHeaderAnchors(mainContent);
      buildPageTOC();
    }

    if (window.__clearSelectionHighlights) window.__clearSelectionHighlights();
    applySwap();
    finalizeNavigation(targetURL, options);
    return Promise.resolve();
  }

  function navigateClientSide(target, options) {
    options = options || {};
    if (!mainContent) {
      window.location.href = target.url.toString();
      return Promise.resolve();
    }

    if (pendingNavigation) {
      pendingNavigation.abort();
    }
    saveScrollPosition(window.location.pathname);
    if (!target.hash && options.scrollToTop !== false && options.restoreScroll !== true) {
      scrollToTopImmediate();
    }
    pendingNavigation = new AbortController();
    var navSignal = pendingNavigation.signal;
    setNavigating(true);
    beginRouteProgress();

    return fetchPagePayload(target.url, navSignal)
      .then(function(payload) {
        if (navSignal.aborted) return;
        var targetURL = new URL(target.url.toString(), window.location.href);
        return swapPageContent(payload, targetURL, {
          updateHistory: options.updateHistory || 'push',
          hash: target.hash,
          scrollToTop: options.scrollToTop,
          restoreScroll: options.restoreScroll === true
        });
      })
      .catch(function(err) {
        if (err && err.name === 'AbortError') return;
        console.warn('[mdpress] Falling back to full navigation', err);
        window.location.href = target.url.toString();
      })
      .finally(function() {
        pendingNavigation = null;
        setNavigating(false);
        endRouteProgress();
      });
  }

  refreshPageContext();
  window.history.replaceState({ path: window.location.pathname }, '', window.location.pathname + window.location.search + window.location.hash);
  syncSidebarForCurrentFile();
  setupHeadingObserver();
  buildPageTOC();
  if (window.__addCopyButtons) window.__addCopyButtons(mainContent);
  updateActiveNavigation();
  ensureMermaid();
  ensureKaTeX();

  // Expose a live-reload hook for the serve WebSocket script.
  // Instead of a full page reload, re-fetch and swap the current page content
  // while preserving the scroll position. This avoids disrupting reading flow
  // when a different chapter is edited.
  window.__mdpressLiveReload = function() {
    // Invalidate all cached pages since any content may have changed.
    for (var key in pageCache) { delete pageCache[key]; }
    pageCacheOrder.length = 0;
    for (var pk in prefetchedPages) { delete prefetchedPages[pk]; }
    if (window.__clearSelectionHighlights) window.__clearSelectionHighlights();
    var savedScroll = window.scrollY || document.documentElement.scrollTop;
    var targetURL = new URL(window.location.href);
    return fetchPagePayload(targetURL)
      .then(function(payload) {
        mainContent.innerHTML = payload.contentHTML;
        document.title = payload.title;
        var breadcrumbNav = document.querySelector('.page-breadcrumb');
        if (breadcrumbNav && payload.breadcrumbHTML) {
          breadcrumbNav.innerHTML = payload.breadcrumbHTML;
        }
        if (window.__addCopyButtons) window.__addCopyButtons(mainContent);
        if (window.__addHeaderAnchors) window.__addHeaderAnchors(mainContent);
        buildPageTOC();
        setupHeadingObserver();
        updateActiveNavigation();
        ensureMermaid();
        ensureKaTeX();
        // Restore scroll position so the reader stays where they were.
        window.scrollTo({ top: savedScroll, behavior: 'auto' });
      })
      .catch(function() {
        // If SPA reload fails, fall back to full page reload.
        location.reload();
      });
  };

  document.addEventListener('mouseover', function(e) {
    var link = e.target.closest('.sidebar-nav a, .page-nav a, .content a');
    if (!link) return;
    prefetchPage(link.href);
    warmPageCache(link.href);
  }, { passive: true });

  document.addEventListener('focusin', function(e) {
    var link = e.target.closest('.sidebar-nav a, .page-nav a, .content a');
    if (!link) return;
    prefetchPage(link.href);
    warmPageCache(link.href);
  });

  document.addEventListener('touchstart', function(e) {
    var link = e.target.closest('.sidebar-nav a, .page-nav a, .content a');
    if (!link) return;
    prefetchPage(link.href);
    warmPageCache(link.href);
  }, { passive: true });

  document.addEventListener('click', function(e) {
    if (e.defaultPrevented) return;
    if (e.button !== 0) return;
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;

    var link = e.target.closest('.sidebar-nav a, .page-nav a, .content a');
    if (!link) return;
    if (link.target && link.target !== '_self') return;
    if (link.hasAttribute('download')) return;

    var target = getClientNavigation(link);
    if (!target) return;

    rememberInternalNavigation(link.href);

    if (target.file === currentFile) {
      if (target.hash) {
        var samePageTarget = document.getElementById(target.hash.slice(1));
        if (samePageTarget) {
          e.preventDefault();
          expandGroupChain(link.closest('.nav-group'));
          scrollToHashTarget(target.hash, true);
          scheduleNavigationUpdate();
        }
      } else if (target.url.pathname === window.location.pathname) {
        e.preventDefault();
        scrollToTopImmediate();
      }
      return;
    }

    e.preventDefault();
    expandGroupChain(link.closest('.nav-group'));
    navigateClientSide(target, {
      updateHistory: 'push',
      scrollToTop: !target.hash
    });
  });

  window.addEventListener('popstate', function() {
    var target = getClientNavigation({ href: window.location.href });
    if (!target) return;
    if (target.file === currentFile) {
      if (target.hash) {
        scrollToHashTarget(target.hash, false);
      } else {
        scrollToTopImmediate();
      }
      scheduleNavigationUpdate();
      return;
    }
    navigateClientSide(target, {
      updateHistory: null,
      scrollToTop: !target.hash,
      restoreScroll: !target.hash
    });
  });

  window.addEventListener('scroll', function() {
    if (scrollSaveFrame !== null) return;
    scrollSaveFrame = window.requestAnimationFrame(function() {
      saveScrollPosition(window.location.pathname);
      scrollSaveFrame = null;
    });
  }, { passive: true });

  // Keyboard navigation: Arrow Left/Right for prev/next, "/" for search
  document.addEventListener('keydown', function(e) {
    var navLinks = document.querySelectorAll('.page-nav a');
    var prevLink = null;
    var nextLink = null;
    for (var i = 0; i < navLinks.length; i++) {
      if (navLinks[i].classList.contains('prev')) prevLink = navLinks[i];
      if (navLinks[i].classList.contains('next')) nextLink = navLinks[i];
    }

    var inEditable = e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.isContentEditable;
    if (e.key === 'ArrowLeft' && prevLink && !e.ctrlKey && !e.metaKey && !e.altKey && !inEditable) {
      e.preventDefault();
      prevLink.click();
    } else if (e.key === 'ArrowRight' && nextLink && !e.ctrlKey && !e.metaKey && !e.altKey && !inEditable) {
      e.preventDefault();
      nextLink.click();
    } else if ((e.key === '/' || (e.key === 'k' && (e.metaKey || e.ctrlKey))) && !inEditable) {
      e.preventDefault();
      openSearch('');
    }
  });

  // Sidebar toggle management
  var sidebarClose = document.querySelector('.sidebar-close');
  var sidebarCollapsedKey = 'mdpress-sidebar-collapsed';

  // Restore collapsed state from localStorage.
  try {
    if (window.localStorage.getItem(sidebarCollapsedKey) === '1') {
      body.classList.add('sidebar-collapsed');
    }
  } catch (e) {}

  function isMobile() { return window.innerWidth <= 768; }

  function toggleSidebar(forceState) {
    if (!sidebar) return;
    if (isMobile()) {
      if (typeof forceState === 'boolean') {
        sidebar.classList.toggle('open', forceState);
        body.classList.toggle('sidebar-open', forceState);
      } else {
        var isOpen = sidebar.classList.toggle('open');
        body.classList.toggle('sidebar-open', isOpen);
      }
    } else {
      var shouldCollapse = typeof forceState === 'boolean' ? forceState : !body.classList.contains('sidebar-collapsed');
      body.classList.toggle('sidebar-collapsed', shouldCollapse);
      try { window.localStorage.setItem(sidebarCollapsedKey, shouldCollapse ? '1' : '0'); } catch (e) {}
    }
    if (sidebarToggle) {
      var expanded = isMobile() ? sidebar.classList.contains('open') : !body.classList.contains('sidebar-collapsed');
      sidebarToggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    }
  }

  if (sidebarToggle) {
    sidebarToggle.addEventListener('click', function(e) {
      e.preventDefault();
      toggleSidebar();
    });
  }

  if (sidebarClose) {
    sidebarClose.addEventListener('click', function(e) {
      e.preventDefault();
      // On mobile, false removes 'open'; on desktop, true sets 'collapsed'.
      toggleSidebar(isMobile() ? false : true);
    });
  }

  // Close sidebar on escape key (skip when search overlay is open)
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape' && sidebar) {
      var searchOverlay = document.getElementById('search-overlay');
      if (searchOverlay && searchOverlay.classList.contains('open')) return;
      if (isMobile() && sidebar.classList.contains('open')) {
        toggleSidebar(false);
      } else if (!isMobile() && !body.classList.contains('sidebar-collapsed')) {
        toggleSidebar(true);
      }
    }
  });

  // Handle sidebar close on overlay click (mobile only)
  document.addEventListener('click', function(e) {
    if (isMobile() && sidebar && sidebar.classList.contains('open') && !sidebar.contains(e.target) && sidebarToggle && !sidebarToggle.contains(e.target)) {
      toggleSidebar(false);
    }
  });

  // Sidebar resize by dragging
  (function() {
    var handle = document.getElementById('sidebar-resize-handle');
    var mainEl = document.querySelector('.main');
    if (!handle || !sidebar || !mainEl) return;
    var sidebarWidthKey = 'mdpress-sidebar-width';
    var minW = 200, maxW = Math.floor(window.innerWidth * 0.5);

    // Restore saved width
    var saved = localStorage.getItem(sidebarWidthKey);
    if (saved) {
      var w = parseInt(saved, 10);
      if (w >= minW && w <= maxW) {
        sidebar.style.width = w + 'px';
        document.documentElement.style.setProperty('--sidebar-width', w + 'px');
      }
    }

    var startX, startW;
    function onMouseMove(e) {
      var newW = Math.min(maxW, Math.max(minW, startW + (e.clientX - startX)));
      sidebar.style.width = newW + 'px';
      document.documentElement.style.setProperty('--sidebar-width', newW + 'px');
    }
    function onMouseUp() {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      handle.classList.remove('active');
      document.body.classList.remove('sidebar-resizing');
      var finalW = parseInt(sidebar.style.width, 10);
      if (finalW) localStorage.setItem(sidebarWidthKey, finalW);
    }
    handle.addEventListener('mousedown', function(e) {
      e.preventDefault();
      startX = e.clientX;
      startW = sidebar.getBoundingClientRect().width;
      maxW = Math.floor(window.innerWidth * 0.5);
      handle.classList.add('active');
      document.body.classList.add('sidebar-resizing');
      document.addEventListener('mousemove', onMouseMove);
      document.addEventListener('mouseup', onMouseUp);
    });
    // Double-click to reset width
    handle.addEventListener('dblclick', function() {
      sidebar.style.width = '';
      document.documentElement.style.setProperty('--sidebar-width', '280px');
      localStorage.removeItem(sidebarWidthKey);
    });
  })();

  /* ===== Full-Text Search ===== */
  (function() {
    var overlay = document.getElementById('search-overlay');
    var modalInput = document.getElementById('search-input');
    var resultsBox = document.getElementById('search-results');
    var searchStatus = document.getElementById('search-status');
    var recentPagesKey = 'mdpress-recent-pages';
    var searchJumpKey = 'mdpress-search-jump';
    var searchIndex = null;
    var searchIndexError = null;
    var activeIdx = -1;
    var debounceTimer = null;

    function loadIndex() {
      if (searchIndex) return Promise.resolve(searchIndex);
      // Remember the failure instead of refetching on every keystroke; under
      // file:// the fetch is guaranteed to keep failing.
      if (searchIndexError) return Promise.reject(searchIndexError);
      return fetch(resolveSiteHref('search-index.json')).then(function(r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      }).then(function(data) {
        searchIndex = data;
        return data;
      }).catch(function(err) {
        // Reject rather than resolving with an empty index. Swallowing this
        // meant that opening _book/index.html straight off disk — where the
        // fetch is rejected as a cross-origin request — answered every query
        // with "No results", telling the reader their content was never
        // indexed when in truth the index was never loaded.
        console.warn('[mdpress] Failed to load search index:', err);
        searchIndexError = err;
        throw err;
      });
    }

    // Whether the page was opened straight off disk. The search index cannot be
    // fetched from a file:// page, so the failure message names the fix.
    function isFileProtocol() {
      return window.location.protocol === 'file:';
    }

    function updateSearchStatus(count) {
      if (!searchStatus) return;
      if (typeof count === 'string') {
        searchStatus.textContent = count;
        return;
      }
      if (count < 0) {
        searchStatus.textContent = '';
        return;
      }
      searchStatus.textContent = count === 1 ? __ui.searchResultsOne : __ui.searchResults.replace('%d', String(count));
    }

    function getRecentPages() {
      try {
        var raw = window.localStorage.getItem(recentPagesKey);
        if (!raw) return [];
        var parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
      } catch (e) {
        return [];
      }
    }

    function saveRecentPage() {
      try {
        var titleNode = document.querySelector('.chapter-title, .content h1');
        var title = titleNode ? titleNode.textContent.trim() : (document.title || '').replace(/\s+-\s+.*$/, '');
        var href = getFileFromPathname(window.location.pathname);
        if (href === 'index.html') return;
        var path = Array.from(document.querySelectorAll('.page-breadcrumb a')).slice(0, -1).map(function(node) { return node.textContent.trim(); }).join(' > ');
        if (!title || !href) return;
        var recent = getRecentPages().filter(function(item) { return item && item.href !== href; });
        recent.unshift({ title: title, href: href, path: path });
        window.localStorage.setItem(recentPagesKey, JSON.stringify(recent.slice(0, 5)));
      } catch (e) {}
    }

    function showSearchJumpNotice() {
      try {
        var raw = sessionStorage.getItem(searchJumpKey);
        if (!raw) return;
        sessionStorage.removeItem(searchJumpKey);
        var query = JSON.parse(raw);
        if (!query) return;
        var content = document.querySelector('.content');
        if (!content) return;
        var notice = document.createElement('div');
        notice.className = 'search-jump-notice';
        notice.textContent = __ui.searchMatched.replace('%s', query);
        content.insertBefore(notice, content.firstChild);
        setTimeout(function() {
          if (notice.parentNode) notice.parentNode.removeChild(notice);
        }, 2400);
      } catch (e) {}
    }

    function renderRecentPages() {
      var recent = getRecentPages();
      if (!recent.length) {
        resultsBox.innerHTML = '<div class="search-empty">' + __ui.recentEmpty + '</div>';
        updateSearchStatus(__ui.recentPages);
        activeIdx = -1;
        return;
      }
      var html = '';
      for (var i = 0; i < recent.length; i++) {
        var item = recent[i];
        html += '<a class="search-result" href="' + escapeHTML(resolveSiteHref(item.href)) + '">';
        if (item.path) html += '<div class="search-result-path">' + escapeHTML(item.path) + '</div>';
        html += '<div class="search-result-title">' + escapeHTML(item.title) + '</div>';
        html += '</a>';
      }
      resultsBox.innerHTML = html;
      activeIdx = 0;
      updateActive(resultsBox.querySelectorAll('.search-result'));
      updateSearchStatus(__ui.recentPages);
    }

    // Header search button opens modal
    var headerSearchBtn = document.getElementById('header-search-btn');
    if (headerSearchBtn) {
      headerSearchBtn.addEventListener('click', function() {
        openSearch('');
      });
    }

    var backdrop = document.getElementById('search-backdrop');

    window.openSearch = function(initialQuery) {
      overlay.classList.add('open');
      backdrop.classList.add('open');
      modalInput.value = initialQuery || '';
      activeIdx = -1;
      loadIndex().catch(function() {});
      if (initialQuery) {
        doSearch();
      } else {
        renderRecentPages();
      }
      requestAnimationFrame(function() { modalInput.focus(); });
    };

    function closeSearch() {
      overlay.classList.remove('open');
      backdrop.classList.remove('open');
      activeIdx = -1;
    }

    backdrop.addEventListener('click', function() {
      closeSearch();
    });

    var leaveTimer = null;
    overlay.addEventListener('mouseleave', function() {
      if (document.activeElement === modalInput) return;
      leaveTimer = setTimeout(closeSearch, 500);
    });
    overlay.addEventListener('mouseenter', function() {
      if (leaveTimer) { clearTimeout(leaveTimer); leaveTimer = null; }
    });

    modalInput.addEventListener('keydown', function(e) {
      if (e.key === 'Escape') { closeSearch(); return; }
      var items = resultsBox.querySelectorAll('.search-result');
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        activeIdx = Math.min(activeIdx + 1, items.length - 1);
        updateActive(items);
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        activeIdx = Math.max(activeIdx - 1, 0);
        updateActive(items);
      } else if (e.key === 'Enter' && activeIdx >= 0 && items[activeIdx]) {
        e.preventDefault();
        items[activeIdx].click();
      }
    });

    function updateActive(items) {
      for (var i = 0; i < items.length; i++) {
        items[i].classList.toggle('search-active', i === activeIdx);
      }
      if (items[activeIdx]) items[activeIdx].scrollIntoView({ block: 'nearest' });
    }

    modalInput.addEventListener('input', function() {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(doSearch, 80);
    });

    function escapeHTML(s) {
      return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    // parseSearchTerms splits a raw query into the terms that must all match.
    // A double-quoted run stays whole, so the exact-phrase syntax the manual
    // documents narrows a search instead of killing it: the quote characters
    // used to survive into the indexOf, so a quoted word searched for a literal
    // string with the quote characters in it that no page can contain. Stray
    // quotes are dropped rather than left to poison the term.
    function parseSearchTerms(raw) {
      var terms = [];
      var re = /"([^"]*)"|(\S+)/g;
      var m;
      while ((m = re.exec(raw)) !== null) {
        var t = (m[1] !== undefined ? m[1] : m[2]).replace(/"/g, '').trim();
        if (t) { terms.push(t); }
      }
      return terms;
    }

    // Han, kana and Hangul - the scripts written without spaces between words.
    var cjkChar = /[㐀-䶿一-鿿豈-﫿぀-ヿ가-힯]/;

    // termMatches reports whether one query term is present in a haystack.
    // A CJK reader types 数据库索引 as one unbroken run, because the script has
    // no spaces to type; requiring that run to appear contiguously returned
    // nothing even when both words were plainly on the page, and there was no
    // way for the reader to guess that inserting ASCII spaces would help. So a
    // term made only of CJK characters also matches when every one of its
    // characters is present - the per-character behavior the manual promises.
    function termMatches(haystack, term) {
      if (haystack.indexOf(term) >= 0) { return true; }
      if (/[a-z0-9]/.test(term) || !cjkChar.test(term)) { return false; }
      for (var i = 0; i < term.length; i++) {
        var c = term.charAt(i);
        if (c !== ' ' && haystack.indexOf(c) < 0) { return false; }
      }
      return true;
    }

    function buildSnippet(text, query, terms) {
      var lower = text.toLowerCase();
      // Prefer the whole query, but fall back to the first term that is really
      // there. A multi-word or CJK match whose words sit apart in the text used
      // to render with no snippet and nothing highlighted.
      var needle = query.toLowerCase();
      var idx = lower.indexOf(needle);
      for (var n = 0; idx < 0 && terms && n < terms.length; n++) {
        needle = terms[n];
        idx = lower.indexOf(needle);
        if (idx < 0 && cjkChar.test(needle)) {
          needle = needle.charAt(0);
          idx = lower.indexOf(needle);
        }
      }
      if (idx < 0) return '';
      var start = Math.max(0, idx - 40);
      var end = Math.min(text.length, idx + needle.length + 80);
      var snippet = (start > 0 ? '\u2026' : '') + text.slice(start, end) + (end < text.length ? '\u2026' : '');
      // Highlight matches — escape both snippet and query so HTML entities
      // in the query (e.g. & -> &amp;) still match their escaped counterparts.
      var escaped = escapeHTML(snippet);
      var escapedNeedle = escapeHTML(needle);
      var re = new RegExp('(' + escapedNeedle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')', 'gi');
      return escaped.replace(re, '<mark>$1</mark>');
    }

    function doSearch() {
      var query = modalInput.value.trim();
      if (!query) {
        renderRecentPages();
        return;
      }
      loadIndex().then(function(index) {
        var qLower = query.toLowerCase();
        // Split into terms and require all of them. A single indexOf on the
        // whole query meant any multi-word search ("plugin hooks") returned
        // nothing, because the words never appear adjacent.
        var terms = parseSearchTerms(qLower);
        if (!terms.length) { terms = [qLower]; }
        function hasAll(haystack) {
          if (!haystack) { return false; }
          var h = haystack.toLowerCase();
          for (var t = 0; t < terms.length; t++) {
            if (!termMatches(h, terms[t])) { return false; }
          }
          return true;
        }
        var matches = [];
        for (var i = 0; i < index.length; i++) {
          var entry = index[i];
          var titleMatch = hasAll(entry.t);
          var pathMatch = hasAll(entry.p || '');
          var textMatch = hasAll(entry.x);
          if (titleMatch || pathMatch || textMatch) {
            matches.push({
              title: entry.t,
              filename: entry.f,
              path: entry.p || '',
              snippet: buildSnippet(entry.x, query, terms),
              titleMatch: titleMatch,
              pathMatch: pathMatch
            });
          }
        }
        // Sort by title > breadcrumb path > body, then cap what is rendered.
        // The cap must come after the sort: truncating the scan first meant a
        // page whose *title* matched was dropped in favor of the first 20
        // body hits in document order, and the reported count was the
        // truncated one.
        matches.sort(function(a, b) {
          var aScore = (a.titleMatch ? 2 : 0) + (a.pathMatch ? 1 : 0);
          var bScore = (b.titleMatch ? 2 : 0) + (b.pathMatch ? 1 : 0);
          return bScore - aScore;
        });
        var totalMatches = matches.length;
        if (matches.length > 20) matches = matches.slice(0, 20);
        updateSearchStatus(totalMatches);

        if (matches.length === 0) {
          resultsBox.innerHTML = '<div class="search-empty">' + __ui.noResults + ' \u201c' + escapeHTML(query) + '\u201d</div>';
          activeIdx = -1;
          return;
        }

        var html = '';
        for (var j = 0; j < matches.length; j++) {
          var m = matches[j];
          var badges = [];
          if (m.titleMatch) badges.push(__ui.searchMatchTitle);
          if (m.pathMatch) badges.push(__ui.searchMatchPath);
          if (!m.titleMatch && !m.pathMatch) badges.push(__ui.searchMatchText);
          html += '<a class="search-result" href="' + escapeHTML(resolveSiteHref(m.filename)) + '">';
          if (badges.length) {
            html += '<div class="search-result-meta">';
            for (var k = 0; k < badges.length; k++) {
              html += '<span class="search-badge">' + escapeHTML(badges[k]) + '</span>';
            }
            html += '</div>';
          }
          if (m.path) html += '<div class="search-result-path">' + escapeHTML(m.path) + '</div>';
          html += '<div class="search-result-title">' + escapeHTML(m.title) + '</div>';
          if (m.snippet) html += '<div class="search-result-snippet">' + m.snippet + '</div>';
          html += '</a>';
        }
        resultsBox.innerHTML = html;
        activeIdx = 0;
        updateActive(resultsBox.querySelectorAll('.search-result'));
      }).catch(function() {
        // The only routine cause is a page opened straight off disk, so name it
        // with the two symbols that need no translation: the protocol that
        // cannot fetch the index, and the command that serves it over http.
        var hint = isFileProtocol()
          ? '<div class="search-empty-hint">file:// → <code>mdpress serve</code></div>'
          : '';
        resultsBox.innerHTML = '<div class="search-empty">' + __ui.searchUnavailable + hint + '</div>';
        updateSearchStatus(-1);
      });
    }

    saveRecentPage();
    window.__saveRecentPage = saveRecentPage;
    window.__showSearchJumpNotice = showSearchJumpNotice;
    showSearchJumpNotice();

    // Click result → navigate via SPA
    resultsBox.addEventListener('click', function(e) {
      var link = e.target.closest('.search-result');
      if (!link) return;
      e.preventDefault();
      closeSearch();
      var href = link.getAttribute('href');
      try {
        sessionStorage.setItem(searchJumpKey, JSON.stringify(modalInput.value.trim()));
      } catch (e) {}
      // Use SPA navigation if available
      if (typeof navigateClientSide === 'function') {
        var target = getClientNavigation({ href: href });
        if (target) {
          navigateClientSide(target, { updateHistory: 'push', scrollToTop: !target.hash });
        } else {
          window.location.href = href;
        }
      } else {
        window.location.href = href;
      }
    });

    // Global ESC closes search
    document.addEventListener('keydown', function(e) {
      if (e.key === 'Escape' && overlay.classList.contains('open')) {
        closeSearch();
      }
    });
  })();

  /* ===== Sticky Header Height ===== */
  (function() {
    /* Anchor targets scroll to scroll-margin-top, which has to match the real
       header height. On a phone the breadcrumb wraps and the header grows well
       past its 64px design height, dropping every anchor jump behind it. */
    var header = document.querySelector('.page-header');
    if (!header) return;
    function measure() {
      var h = Math.round(header.getBoundingClientRect().height);
      if (h > 0) {
        document.documentElement.style.setProperty('--header-h', h + 'px');
      }
    }
    measure();
    if (window.ResizeObserver) {
      new ResizeObserver(measure).observe(header);
    } else {
      window.addEventListener('resize', measure);
    }
    window.addEventListener('load', measure);
  })();

  /* ===== Code Block Copy Buttons ===== */
  (function() {
    function codeLanguage(pre) {
      var code = pre.querySelector('code');
      var lang = pre.getAttribute('data-lang') || (code && code.getAttribute('data-lang')) || '';
      if (!lang && code) {
        var match = /(?:^|\s)language-([^\s]+)/.exec(code.className || '');
        if (match) lang = match[1];
      }
      return lang === 'text' ? '' : lang;
    }

    function addCopyButtons(root) {
      var pres = (root || document).querySelectorAll('pre');
      for (var i = 0; i < pres.length; i++) {
        var pre = pres[i];
        if (pre.parentNode.classList.contains('code-wrapper')) continue;
        var wrapper = document.createElement('div');
        wrapper.className = 'code-wrapper';
        /* Surface the fence language the renderer recorded, so a reader can
           tell a shell transcript from its output the way they can in the
           standalone HTML build. */
        var lang = codeLanguage(pre);
        if (lang) wrapper.setAttribute('data-lang', lang);
        pre.parentNode.insertBefore(wrapper, pre);
        wrapper.appendChild(pre);
        var btn = document.createElement('button');
        btn.className = 'copy-btn';
        btn.textContent = __ui.copy;
        btn.type = 'button';
        btn.setAttribute('aria-label', __ui.copy);
        wrapper.appendChild(btn);
      }
    }
    document.addEventListener('click', function(e) {
      var btn = e.target.closest('.copy-btn');
      if (!btn) return;
      var pre = btn.parentNode.querySelector('pre');
      if (!pre) return;
      var text = pre.textContent || pre.innerText;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function() {
          btn.textContent = __ui.copied;
          btn.classList.add('copied');
          setTimeout(function() { btn.textContent = __ui.copy; btn.classList.remove('copied'); }, 2000);
        }).catch(function() {});
      } else {
        try {
          var ta = document.createElement('textarea');
          ta.value = text;
          ta.style.position = 'fixed';
          ta.style.opacity = '0';
          document.body.appendChild(ta);
          ta.select();
          document.execCommand('copy');
          document.body.removeChild(ta);
          btn.textContent = __ui.copied;
          btn.classList.add('copied');
          setTimeout(function() { btn.textContent = __ui.copy; btn.classList.remove('copied'); }, 2000);
        } catch(err) {}
      }
    });
    addCopyButtons();
    window.__addCopyButtons = addCopyButtons;
  })();

  /* ===== Selection Highlight ===== */
  (function() {
    var highlightClass = 'selection-highlight';
    var minLen = 2;
    var maxLen = 100;
    var debounceTimer = null;
    var updating = false;

    function clearHighlights() {
      var marks = document.querySelectorAll('.' + highlightClass);
      for (var i = marks.length - 1; i >= 0; i--) {
        var mark = marks[i];
        var parent = mark.parentNode;
        parent.replaceChild(document.createTextNode(mark.textContent), mark);
        parent.normalize();
      }
    }

    function escapeRegExp(s) {
      return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    function highlightInNode(node, regex) {
      if (node.nodeType === 3) {
        regex.lastIndex = 0;
        var match = regex.exec(node.textContent);
        if (match) {
          var span = document.createElement('span');
          span.className = highlightClass;
          var mid = node.splitText(match.index);
          mid.splitText(match[0].length);
          span.appendChild(mid.cloneNode(true));
          mid.parentNode.replaceChild(span, mid);
          return true;
        }
        return false;
      }
      if (node.nodeType === 1) {
        var tag = node.tagName;
        if (tag === 'SCRIPT' || tag === 'STYLE' || tag === 'TEXTAREA' || tag === 'INPUT' ||
            node.classList.contains(highlightClass) || node.classList.contains('copy-btn') ||
            node.isContentEditable) return false;
        for (var i = 0; i < node.childNodes.length; i++) {
          if (highlightInNode(node.childNodes[i], regex)) {
            i++; // skip the newly inserted span
          }
        }
      }
      return false;
    }

    // Compute character offset of selection start within a <pre>.
    function getSelOffset(pre, sel) {
      var range = sel.getRangeAt(0);
      var r2 = document.createRange();
      r2.selectNodeContents(pre);
      r2.setEnd(range.startContainer, range.startOffset);
      return { pre: pre, offset: r2.toString().length, length: sel.toString().length };
    }

    // Restore selection by character offset after DOM was restructured.
    function restoreSelOffset(info) {
      var walker = document.createTreeWalker(info.pre, NodeFilter.SHOW_TEXT);
      var pos = 0, startNode, startOff, endNode, endOff;
      var target = info.offset, end = info.offset + info.length;
      while (walker.nextNode()) {
        var n = walker.currentNode, len = n.textContent.length;
        if (!startNode && pos + len > target) { startNode = n; startOff = target - pos; }
        if (!endNode && pos + len >= end) { endNode = n; endOff = end - pos; break; }
        pos += len;
      }
      if (startNode && endNode) {
        var sel = window.getSelection();
        var r = document.createRange();
        r.setStart(startNode, startOff);
        r.setEnd(endNode, endOff);
        sel.removeAllRanges();
        sel.addRange(r);
      }
    }

    function onSelectionChange() {
      if (updating) return;
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(function() {
        updating = true;
        try {
          clearHighlights();
          var sel = window.getSelection();
          if (!sel || sel.isCollapsed || sel.rangeCount === 0) return;
          // Only activate when selection originates inside a code block
          var anchor = sel.anchorNode;
          var selPre = anchor && anchor.parentElement && anchor.parentElement.closest('pre');
          if (!selPre) return;
          var text = sel.toString().trim();
          if (text.length < minLen || text.length > maxLen) return;
          if (/^\s*$/.test(text)) return;
          // Save selection as character offset (survives DOM restructuring)
          var saved = getSelOffset(selPre, sel);
          // Highlight in all code blocks on the page
          var pres = document.querySelectorAll('.content pre');
          var regex = new RegExp(escapeRegExp(text), 'gi');
          for (var i = 0; i < pres.length; i++) {
            highlightInNode(pres[i], regex);
          }
          // Restore selection by offset
          try { restoreSelOffset(saved); } catch(e) {}
        } finally {
          updating = false;
        }
      }, 300);
    }

    document.addEventListener('selectionchange', onSelectionChange);
    window.__clearSelectionHighlights = function() {
      clearHighlights();
      if (debounceTimer) clearTimeout(debounceTimer);
    };
  })();
