// AI-MONITOR · main app
// Wires sections together, scroll-reveal, command palette, tweaks.

function App() {
  const [t, setTweak] = useTweaks(TWEAK_DEFAULTS);
  const [cmd, setCmd] = useState(false);
  const { t: tr, lang } = useLang();
  useEffect(() => {
    document.title = tr("Home AI Compute Monitor");
    document.documentElement.lang = lang;
  }, [lang]);

  // Apply tweaks to <html>
  useEffect(() => {
    const root = document.documentElement;
    root.dataset.theme = t.theme;
    root.dataset.density = t.density;
    root.dataset.motion = t.motion ? "1" : "0";
    root.style.setProperty("--accent", t.accent);
  }, [t.theme, t.density, t.motion, t.accent]);

  // Live tick interval from tweaks
  useEffect(() => {
    window.AIData.setIntervalMs(t.tickMs);
  }, [t.tickMs]);

  // Data source: auto | mock | live
  useEffect(() => {
    if (t.dataSource === "auto") {
      localStorage.removeItem("aim.mode");
      // Don't force a switch — let the initial auto-probe decide; user can
      // reload the page to re-probe. (Switching to a fresh SSE connection
      // on every toggle is jittery.)
    } else if (t.dataSource !== window.AIData.mode) {
      window.AIData.switchMode(t.dataSource);
    }
  }, [t.dataSource]);

  // ⌘K / Ctrl-K
  useEffect(() => {
    const onKey = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setCmd((v) => !v);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Scroll reveal — only for sections; hero is already in.
  useEffect(() => {
    const els = document.querySelectorAll(".reveal:not(.in)");
    const io = new IntersectionObserver((entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting) {
          e.target.classList.add("in");
          io.unobserve(e.target);
        }
      });
    }, { threshold: 0.08, rootMargin: "0px 0px -8% 0px" });
    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);

  // Active nav-link tracking on scroll
  useEffect(() => {
    const ids = ["overview","cluster","nodes","models","training","telemetry","fabric"];
    const links = Array.from(document.querySelectorAll(".nav-links a"));
    const sections = ids.map((id) => document.getElementById(id)).filter(Boolean);
    const onScroll = () => {
      const y = window.scrollY + 80;
      let activeId = "overview";
      for (const s of sections) {
        if (s.offsetTop <= y) activeId = s.id;
      }
      links.forEach((l) => {
        if (l.getAttribute("href") === "#" + activeId) l.setAttribute("data-active", "");
        else l.removeAttribute("data-active");
      });
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  return (
    <>
      <div className="field" />
      <Nav onOpenCmd={() => setCmd(true)} />
      <main>
        <Hero />
        <Cluster />
        <NodesSection />
        <ModelsSection />
        <TrainingSection />
        <TelemetrySection />
        <FabricSection />
        <footer className="foot">
          <span>{tr("Home AI Compute Monitor")}</span>
          <span>{tr("Prometheus 2.55 · DCGM 3.3 · LiteLLM 1.52 · scrape 15 s · retention 7 d")}</span>
        </footer>
      </main>

      <CmdK open={cmd} onClose={() => setCmd(false)} />

      <TweaksPanel title={tr("Tweaks")}>
        <TweakSection label={tr("Appearance")} />
        <TweakRadio label={tr("Theme")} value={t.theme}
                    options={[{value:"dark",label:tr("Dark")},{value:"light",label:tr("Light")}]}
                    onChange={(v) => setTweak("theme", v)} />
        <TweakColor label={tr("Accent")} value={t.accent}
                    options={["#0a84ff", "#bf5af2", "#ff375f", "#30d158", "#ffd60a"]}
                    onChange={(v) => setTweak("accent", v)} />
        <TweakRadio label={tr("Density")} value={t.density}
                    options={[{value:"compact",label:tr("Compact")},{value:"regular",label:tr("Regular")},{value:"comfy",label:tr("Comfy")}]}
                    onChange={(v) => setTweak("density", v)} />

        <TweakSection label={tr("Live data")} />
        <TweakRadio label={tr("Source")} value={t.dataSource}
                    options={[{value:"auto",label:tr("Auto")},{value:"live",label:tr("Live")},{value:"mock",label:tr("Mock")}]}
                    onChange={(v) => setTweak("dataSource", v)} />
        <TweakSlider label={tr("Tick")} value={t.tickMs} min={300} max={3000} step={100} unit=" ms"
                     onChange={(v) => setTweak("tickMs", v)} />
        <TweakToggle label={tr("Motion / animations")} value={t.motion}
                     onChange={(v) => setTweak("motion", v)} />

        <TweakSection label={tr("Shortcut")} />
        <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "rgba(41,38,27,.65)", lineHeight: 1.6 }}>
          {tr("Press")} <span className="kbd" style={{ color: "rgba(41,38,27,.85)" }}>⌘K</span> {tr("anywhere to open the command palette.")}<br />
          {tr("Click any node or model row for the forensic view.")}
        </div>
      </TweaksPanel>
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
