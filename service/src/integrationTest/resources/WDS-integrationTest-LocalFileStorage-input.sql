-- This is a representative WDS backup file, generated by pg_dump.
-- It contains one pg schema (aka WDS collection), which
-- has one table. Summary of the contents:
--
-- schema 10000000-0000-0000-0000-000000000111
--     table "thing"


--
-- PostgreSQL database dump
--

-- Dumped from database version 14.8 (Debian 14.8-1.pgdg110+1)
-- Dumped by pg_dump version 15.3

-- Started on 2023-07-19 09:45:20 EDT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 7 (class 2615 OID 16431)
-- Name: 10000000-0000-0000-0000-000000000111; Type: SCHEMA; Schema: -; Owner: wds
--

CREATE SCHEMA "10000000-0000-0000-0000-000000000111";


ALTER SCHEMA "10000000-0000-0000-0000-000000000111" OWNER TO wds;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 217 (class 1259 OID 16432)
-- Name: thing; Type: TABLE; Schema: 10000000-0000-0000-0000-000000000111; Owner: wds
--

CREATE TABLE "10000000-0000-0000-0000-000000000111".thing (
                                                              the_id text NOT NULL,
                                                              acolumn text
);


ALTER TABLE "10000000-0000-0000-0000-000000000111".thing OWNER TO wds;

--
-- TOC entry 3335 (class 0 OID 16432)
-- Dependencies: 217
-- Data for Name: thing; Type: TABLE DATA; Schema: 10000000-0000-0000-0000-000000000111; Owner: wds
--

COPY "10000000-0000-0000-0000-000000000111".thing (the_id, acolumn) FROM stdin;
1234567	abcdefg
7654321	gfedcba
\.


--
-- TOC entry 3195 (class 2606 OID 16438)
-- Name: thing thing_pkey; Type: CONSTRAINT; Schema: 10000000-0000-0000-0000-000000000111; Owner: wds
--

ALTER TABLE ONLY "10000000-0000-0000-0000-000000000111".thing
    ADD CONSTRAINT thing_pkey PRIMARY KEY (the_id);


-- Completed on 2023-07-19 09:45:22 EDT

--
-- PostgreSQL database dump complete
--

