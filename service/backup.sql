--
-- PostgreSQL database dump
--

-- Dumped from database version 13.1 (Debian 13.1-1.pgdg100+1)
-- Dumped by pg_dump version 14.8 (Ubuntu 14.8-0ubuntu0.22.04.1)

-- Started on 2023-06-01 14:56:33 PDT

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
-- TOC entry 5 (class 2615 OID 43396)
-- Name: 123e4567-e89b-12d3-a456-426614174000; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA "123e4567-e89b-12d3-a456-426614174000";


ALTER SCHEMA "123e4567-e89b-12d3-a456-426614174000" OWNER TO postgres;

--
-- TOC entry 7 (class 2615 OID 16397)
-- Name: sys_wds; Type: SCHEMA; Schema: -; Owner: wds
--

CREATE SCHEMA sys_wds;


ALTER SCHEMA sys_wds OWNER TO wds;

--
-- TOC entry 643 (class 1247 OID 16405)
-- Name: array_of_file; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.array_of_file AS text[];


ALTER DOMAIN public.array_of_file OWNER TO wds;

--
-- TOC entry 637 (class 1247 OID 16401)
-- Name: array_of_relation; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.array_of_relation AS text[];


ALTER DOMAIN public.array_of_relation OWNER TO wds;

--
-- TOC entry 640 (class 1247 OID 16403)
-- Name: file; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.file AS text;


ALTER DOMAIN public.file OWNER TO wds;

--
-- TOC entry 634 (class 1247 OID 16399)
-- Name: relation; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.relation AS text;


ALTER DOMAIN public.relation OWNER TO wds;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 203 (class 1259 OID 16391)
-- Name: databasechangelog; Type: TABLE; Schema: public; Owner: wds
--

CREATE TABLE public.databasechangelog (
    id character varying(255) NOT NULL,
    author character varying(255) NOT NULL,
    filename character varying(255) NOT NULL,
    dateexecuted timestamp without time zone NOT NULL,
    orderexecuted integer NOT NULL,
    exectype character varying(10) NOT NULL,
    md5sum character varying(35),
    description character varying(255),
    comments character varying(255),
    tag character varying(255),
    liquibase character varying(20),
    contexts character varying(255),
    labels character varying(255),
    deployment_id character varying(10)
);


ALTER TABLE public.databasechangelog OWNER TO wds;

--
-- TOC entry 202 (class 1259 OID 16386)
-- Name: databasechangeloglock; Type: TABLE; Schema: public; Owner: wds
--

CREATE TABLE public.databasechangeloglock (
    id integer NOT NULL,
    locked boolean NOT NULL,
    lockgranted timestamp without time zone,
    lockedby character varying(255)
);


ALTER TABLE public.databasechangeloglock OWNER TO wds;

--
-- TOC entry 204 (class 1259 OID 16406)
-- Name: instance; Type: TABLE; Schema: sys_wds; Owner: wds
--

CREATE TABLE sys_wds.instance (
    id uuid NOT NULL
);


ALTER TABLE sys_wds.instance OWNER TO wds;

--
-- TOC entry 2959 (class 0 OID 16391)
-- Dependencies: 203
-- Data for Name: databasechangelog; Type: TABLE DATA; Schema: public; Owner: wds
--

COPY public.databasechangelog (id, author, filename, dateexecuted, orderexecuted, exectype, md5sum, description, comments, tag, liquibase, contexts, labels, deployment_id) FROM stdin;
20230426_syswds_schema	davidan	liquibase/changesets/20230426_syswds_schema.yaml	2023-05-23 16:43:54.48295	1	EXECUTED	8:b29510f962527373c017911411232c5a	sql		\N	4.21.1	\N	\N	4885434439
20230426_syswds_schema	davidan	liquibase/changesets/20230426_domains.yaml	2023-05-23 16:43:54.497692	2	EXECUTED	8:23c121fb77a7a2dcbf2761b7cf3851b3	sql		\N	4.21.1	\N	\N	4885434439
20230426_syswds_schema	davidan	liquibase/changesets/20230426_instance_table.yaml	2023-05-23 16:43:54.509237	3	EXECUTED	8:2d6affed0e524144ac539739229d7529	createTable tableName=instance		\N	4.21.1	\N	\N	4885434439
\.


--
-- TOC entry 2958 (class 0 OID 16386)
-- Dependencies: 202
-- Data for Name: databasechangeloglock; Type: TABLE DATA; Schema: public; Owner: wds
--

COPY public.databasechangeloglock (id, locked, lockgranted, lockedby) FROM stdin;
1	f	\N	\N
\.


--
-- TOC entry 2960 (class 0 OID 16406)
-- Dependencies: 204
-- Data for Name: instance; Type: TABLE DATA; Schema: sys_wds; Owner: wds
--

COPY sys_wds.instance (id) FROM stdin;
123e4567-e89b-12d3-a456-426614174000
\.


--
-- TOC entry 2825 (class 2606 OID 16390)
-- Name: databasechangeloglock databasechangeloglock_pkey; Type: CONSTRAINT; Schema: public; Owner: wds
--

ALTER TABLE ONLY public.databasechangeloglock
    ADD CONSTRAINT databasechangeloglock_pkey PRIMARY KEY (id);


--
-- TOC entry 2827 (class 2606 OID 16410)
-- Name: instance instance_pkey; Type: CONSTRAINT; Schema: sys_wds; Owner: wds
--

ALTER TABLE ONLY sys_wds.instance
    ADD CONSTRAINT instance_pkey PRIMARY KEY (id);


-- Completed on 2023-06-01 14:56:33 PDT

--
-- PostgreSQL database dump complete
--

