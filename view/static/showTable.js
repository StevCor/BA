// Funktion zur Anzeige der Grid.js-Tabellen, mit Anpassungen (v. a. Übersetzungen) übernommen von https://blog.miguelgrinberg.com/post/beautiful-interactive-tables-for-your-flask-templates
function showTable(columnList, data, tableId) {
    new gridjs.Grid({
        columns: columnList,
        data: data,
        search: false,
        sort: true,
        pagination: true,
        language: {
            'search': {
                'placeholder': '🔍 Suche...'
            },
            'pagination': {
                'previous': 'Vorige',
                'next': 'Nächste',
                navigate: (page, pages) => `Seite ${page} von ${pages}`,
                page: (page) => `Seite ${page}`,
                'showing': 'Zeige',
                of: 'von',
                to: 'bis',
                'results': () => 'Einträgen'
            },
            loading: 'Lade...',
            noRecordsFound: 'Keine passenden Einträge gefunden',
            error: 'Beim Einlesen der Daten ist ein Fehler aufgetreten.',
        }
    }).render(document.getElementById(tableId));
}