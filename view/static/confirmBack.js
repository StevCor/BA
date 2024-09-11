// Funktion zur Abfrage einer Bestätigung, wenn ein Zurück-Button betätigt wird
function confirmBack() {
    var result = confirm('Sind Sie sicher, dass Sie zur vorigen Seite zurückkehren wollen?');
    if (result) {
        window.history.back();
    }
}