// Funktion zur Abfrage einer Bestätigung, ob der eingegene Wert absichtlich leer bzw. None/NULL ist
function isEmpty(inputTextElement) {
    if (inputTextElement.value === '' || inputTextElement.value === 'None' || inputTextElement.value == 'NULL') {
        if (confirm('Sind Sie sicher, dass Sie die ausgewählten Werte löschen wollen?') == true) {
            return true;
        } else {
            return false;
        }
    }
    // Hat der eingegebene String einen anderen Wert, wird keine Bestätigung abgefragt, sondern der sich anschließende Code ausgeführt.
    return true;
}
