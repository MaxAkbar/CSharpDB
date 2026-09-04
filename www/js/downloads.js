// Page-local copy feedback; downloads and instructions work without JavaScript.
document.addEventListener('click', async (event) => {
    const button = event.target.closest('[data-copy-target]');
    if (!button) return;
    const command = document.getElementById(button.dataset.copyTarget);
    const status = button.parentElement.querySelector('[role="status"]');
    if (!command || !status) return;
    try {
        await navigator.clipboard.writeText(command.textContent.trim());
        status.textContent = 'Copied.';
    } catch {
        status.textContent = 'Could not copy. Select and copy the command above.';
    }
});
