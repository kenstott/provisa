-- Seed for tests/integration/test_hasura_v2_live_import.py (REQ-1680, REQ-1681, REQ-1682).
-- The tables Hasura's metadatautil sample (cli/internal/metadatautil/testdata/json/t1) tracks:
-- a snake_case Chinook subset, plus the two SQL functions the sample tracks.
DROP TABLE IF EXISTS playlist_track, tracks, playlists, media_types, genres, albums, artists CASCADE;
DROP FUNCTION IF EXISTS search_albums(text);
DROP FUNCTION IF EXISTS search_artists(text);

CREATE TABLE artists (id integer PRIMARY KEY, name text NOT NULL);
CREATE TABLE albums (id integer PRIMARY KEY, title text NOT NULL, artist_id integer NOT NULL REFERENCES artists(id));
CREATE TABLE genres (id integer PRIMARY KEY, name text NOT NULL);
CREATE TABLE media_types (id integer PRIMARY KEY, name text NOT NULL);
CREATE TABLE playlists (id integer PRIMARY KEY, name text NOT NULL);
CREATE TABLE tracks (
  id integer PRIMARY KEY,
  name text NOT NULL,
  album_id integer REFERENCES albums(id),
  genre_id integer REFERENCES genres(id),
  media_type_id integer REFERENCES media_types(id),
  milliseconds integer NOT NULL,
  unit_price numeric(10,2) NOT NULL
);
CREATE TABLE playlist_track (
  playlist_id integer NOT NULL REFERENCES playlists(id),
  track_id integer NOT NULL REFERENCES tracks(id),
  PRIMARY KEY (playlist_id, track_id)
);

INSERT INTO artists VALUES (1, 'AC/DC'), (2, 'Accept'), (3, 'Aerosmith');
INSERT INTO albums VALUES
  (1, 'For Those About To Rock We Salute You', 1),
  (2, 'Balls to the Wall', 2),
  (3, 'Restless and Wild', 2),
  (4, 'Let There Be Rock', 1);
INSERT INTO genres VALUES (1, 'Rock'), (2, 'Jazz');
INSERT INTO media_types VALUES (1, 'MPEG audio file');
INSERT INTO playlists VALUES (1, 'Music'), (2, 'Movies');
INSERT INTO tracks VALUES
  (1, 'For Those About To Rock (We Salute You)', 1, 1, 1, 343719, 0.99),
  (2, 'Balls to the Wall', 2, 1, 1, 342562, 0.99),
  (3, 'Fast As a Shark', 3, 1, 1, 230619, 0.99),
  (4, 'Restless and Wild', 3, 1, 1, 252051, 0.99),
  (5, 'Let There Be Rock', 4, 1, 1, 366654, 0.99);
INSERT INTO playlist_track VALUES (1, 1), (1, 2), (1, 5), (2, 3);

CREATE FUNCTION search_albums(search text) RETURNS SETOF albums AS $$
  SELECT * FROM albums WHERE title ILIKE '%' || search || '%'
$$ LANGUAGE sql STABLE;

CREATE FUNCTION search_artists(search text) RETURNS SETOF artists AS $$
  SELECT * FROM artists WHERE name ILIKE '%' || search || '%'
$$ LANGUAGE sql STABLE;
