(() => {
  const button = document.querySelector('[aria-label="Open documentation index"]');
  const sidebar = document.querySelector('[data-slot="sidebar-container"]');
  if (!button || !sidebar) return;

  const close = () => {
    document.body.classList.remove('docs-nav-open');
    button.setAttribute('aria-expanded', 'false');
  };

  button.setAttribute('aria-expanded', 'false');
  button.addEventListener('click', () => {
    const open = document.body.classList.toggle('docs-nav-open');
    button.setAttribute('aria-expanded', String(open));
  });
  sidebar.querySelectorAll('a').forEach((link) => link.addEventListener('click', close));
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') close();
  });
})();
