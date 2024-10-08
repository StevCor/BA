-- Anlegen der Datenbank PostgreTest1
CREATE DATABASE "PostgresTest1" WITH ENCODING 'UTF8'; 

---- Im Anschluss hieran muss zuerst eine Verbindung zu dieser Datenbank aufgebaut werden (z. B. über das Dropdown-Menü im Query-Tool von pgAdmin) -----

-- Anlegen der Tabelle Vorlesung_Datenbanken_SS2024
CREATE TABLE IF NOT EXISTS "Vorlesung_Datenbanken_SS2024" (
  "Matrikelnummer" INTEGER NOT NULL CHECK ("Matrikelnummer" between 1000000 and 9999999),
  "Vorname" VARCHAR(32) NOT NULL,
  "Nachname" VARCHAR(32) NOT NULL,
  PRIMARY KEY ("Matrikelnummer")
);
INSERT INTO "Vorlesung_Datenbanken_SS2024" ("Matrikelnummer", "Vorname", "Nachname") VALUES
	(1869972, 'Kevin', 'Ganter'),
	(1912967, 'Joanna', 'Hayes'),
	(1938205, 'Carla', 'Jones'),
	(1972793, 'Lisa', 'Johnson'),
	(2021596, 'Renee', 'Bach'),
	(2076750, 'Gregor', 'Hall'),
	(2120434, 'Charles', 'Ferguson'),
	(2192140, 'Steven', 'Decker'),
	(2256812, 'Kaitlyn', 'Kyle'),
	(2261095, 'Anita', 'Smith'),
	(2262911, 'Daniel', 'Wall'),
	(2302766, 'Alicia', 'Melton'),
	(2320350, 'Jim', 'Hampton'),
	(2453099, 'Anton', 'Gerhardt'),
	(2454294, 'Belinda', 'Müller'),
	(2507172, 'Sarah', 'Barnett'),
	(2510983, 'Nicole', 'Meyer'),
	(2643692, 'Benjamin', 'Patel'),
	(2695599, 'Joel', 'Turner'),
	(2703748, 'Diana', 'Pohl'),
	(2752103, 'Siobhan', 'O''Brien'),
	(2814068, 'Nancy', 'Schmitt'),
	(2834378, 'Keith', 'Chan'),
	(2838526, 'Joyce', 'Edwards'),
	(2885172, 'Katie', 'Melcher'),
	(2929136, 'Matthew', 'Connor'),
	(2985690, 'Adam', 'Jones'),
	(3078691, 'Angela', 'Dominguez'),
	(3107857, 'Heather', 'Smith'),
	(3132351, 'Kristin', 'Escobar'),
	(3161520, 'Bernard', 'Müller'),
	(3270214, 'Ashley', 'Pinot'),
	(3289951, 'Jennifer', 'Meyer'),
	(3299826, 'Jason', 'Selle'),
	(3407967, 'Carolyn', 'Taylor'),
	(3442435, 'Alexandra', 'Flanders'),
	(3531772, 'Cristina', 'Smith'),
	(3609446, 'Denise', 'Graham'),
	(3666013, 'Barbara', 'Payne'),
	(3763593, 'Waltraud', 'Gärtner'),
	(3832259, 'Angela', 'Stahl'),
	(3833953, 'Jeffrey', 'Lynch'),
	(3889081, 'Andrea', 'Johnson'),
	(4058208, 'Cindy', 'Davis'),
	(4143847, 'Lauren', 'Sharp'),
	(4150993, 'Jonathan', 'Fox'),
	(4275518, 'Amanda', 'Martinez'),
	(4490484, 'Joseph', 'Robinson'),
	(5181568, 'Jay', 'Pierce'),
	(5234568, 'Denise', 'Estrada'),
	(7663912, 'Gloria', 'Robles');
	
-- Anlegen der Tabelle Uebung_Datenbanken_SS2024
CREATE TABLE IF NOT EXISTS "Uebung_Datenbanken_SS2024" (
	"Matrikelnummer" INTEGER NOT NULL CHECK("Matrikelnummer" BETWEEN 1000000 AND 9999999),
	"Punktzahl" INTEGER NOT NULL DEFAULT 0 CHECK("Punktzahl" BETWEEN 0 AND 250),
	zugelassen BOOLEAN DEFAULT FALSE,
	PRIMARY KEY("Matrikelnummer")
);
INSERT INTO "Uebung_Datenbanken_SS2024" ("Matrikelnummer", "Punktzahl", zugelassen) VALUES
	(1869972, 0, FALSE),
	(1912967, 15, FALSE),
	(1938205, 200, TRUE),
	(1972793, 25, FALSE),
	(2021596, 120, FALSE),
	(2076750, 100, FALSE),
	(2120434, 54, FALSE),
	(2192140, 75, FALSE),
	(2256812, 200, TRUE),
	(2261095, 210, TRUE),
	(2262911, 168, FALSE),
	(2302766, 150, FALSE),
	(2320350, 210, TRUE),
	(2453099, 175, TRUE),
	(2454294, 63, FALSE),
	(2493020, 105, FALSE),
	(2507172, 97, FALSE),
	(2510983, 167, TRUE),
	(2643692, 233, TRUE),
	(2695599, 75, FALSE),
	(2703748, 75, FALSE),
	(2752103, 85, FALSE),
	(2814068, 0, FALSE),
	(2834378, 200, TRUE),
	(2838526, 100, FALSE),
	(2885172, 132, FALSE),
	(2929136, 128, FALSE),
	(2985690, 80, FALSE),
	(3078691, 65, FALSE),
	(3609446, 142, FALSE),
	(3632897, 210, TRUE),
	(3763593, 200, TRUE),
	(5181568, 175, TRUE);
	
-- Anlegen der Tabelle vorlesung_datenbanken_ss2023 (als Beispiel für eine bereits zusammengefügte Tabelle)		
CREATE TABLE IF NOT EXISTS "Vorlesung_Datenbanken_SS2023" (
  "Matrikelnummer" INTEGER NOT NULL CHECK ("Matrikelnummer" between 1000000 and 9999999),
  "Vorname" VARCHAR(32) NOT NULL,
  "Nachname" VARCHAR(32) NOT NULL,
  zugelassen BOOLEAN DEFAULT FALSE,
  "Note" VARCHAR(16) DEFAULT NULL,
  PRIMARY KEY ("Matrikelnummer")
);
INSERT INTO "Vorlesung_Datenbanken_SS2023" ("Matrikelnummer", "Vorname", "Nachname", zugelassen, "Note")
VALUES (1432209, 'Hendrik', 'Nielsen', TRUE, '1.0'),
(1503456, 'Jessica', 'Wolnitz', FALSE, NULL),
(2000675, 'Anton', 'Hegl', FALSE, NULL),
(2111098, 'Zara', 'Lohefalter', TRUE, '4.0'),
(2233449, 'Tatiana', 'Hatt', FALSE, NULL),
(2340992, 'Carlos', 'Metzger', TRUE, '2.7'),
(2345644, 'Tristan', 'Ingwersen', TRUE, '5.0'),
(2356781, 'Benedikt', 'Friedrichs', TRUE, 'n.b.'),
(2360099, 'Gustav', 'Grant', TRUE, 'n. b.'),
(2398562, 'Karl', 'Heinz', TRUE, '2.7'),
(2400563, 'Gudrun', 'Becker', FALSE, NULL);

