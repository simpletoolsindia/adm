// Download links from the latest GitHub release, the visitor's OS highlighted, and the gallery.
(() => {
  const repo = document.documentElement.dataset.repo;
  const releases = `https://github.com/${repo}/releases`;
  for (const [id, href] of [["all-releases", releases], ["repo-link", `https://github.com/${repo}`], ["issues-link", `https://github.com/${repo}/issues`]]) {
    const a = document.getElementById(id);
    if (a) a.href = href;
  }

  // Until the release list loads (or if it can't), every link goes to the releases page.
  const assetLinks = [...document.querySelectorAll("a[data-asset]")];
  for (const a of assetLinks) a.href = `${releases}/latest`;

  const ua = navigator.userAgent;
  const os = /Windows/i.test(ua) ? "windows" : /Mac OS X|Macintosh/i.test(ua) ? "mac" : /Linux|X11|CrOS/i.test(ua) && !/Android/i.test(ua) ? "linux" : null;
  const names = { windows: "Windows", mac: "macOS", linux: "Linux" };
  const card = os && document.querySelector(`.platform[data-os="${os}"]`);
  const hero = document.getElementById("hero-download");
  if (card) {
    card.classList.add("current");
    hero.querySelector("span").textContent = `Download for ${names[os]}`;
  }

  fetch(`https://api.github.com/repos/${repo}/releases/latest`, { headers: { Accept: "application/vnd.github+json" } })
    .then((r) => (r.ok ? r.json() : Promise.reject(r.status)))
    .then((release) => {
      const assets = release.assets || [];
      for (const a of assetLinks) {
        const found = assets.find((x) => new RegExp(a.dataset.asset, "i").test(x.name));
        if (found) {
          a.href = found.browser_download_url;
          a.querySelector("span").textContent = `${a.querySelector("span").textContent} · ${(found.size / 1048576).toFixed(1)} MB`;
        } else {
          a.hidden = true;
        }
      }
      const line = document.getElementById("release-line");
      const date = new Date(release.published_at).toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });
      line.textContent = `Version ${release.tag_name.replace(/^v/, "")}, released ${date}. The browser extension is included in every download.`;
      const best = card && card.querySelector("a[data-asset]:not([hidden])");
      if (best) hero.href = best.href;
    })
    .catch(() => {
      // No release yet, or the API is rate-limited: the releases page links stay.
    });

  // Screenshot tabs.
  const img = document.getElementById("gallery-img");
  const caption = document.getElementById("gallery-caption");
  const tabs = [...document.querySelectorAll(".tabs button")];
  for (const tab of tabs) {
    tab.addEventListener("click", () => {
      for (const t of tabs) t.setAttribute("aria-selected", String(t === tab));
      img.src = tab.dataset.img;
      caption.textContent = tab.dataset.caption;
    });
  }
})();
