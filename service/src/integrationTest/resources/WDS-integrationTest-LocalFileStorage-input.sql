--
-- PostgreSQL database dump
--

-- Dumped from database version 14.8 (Debian 14.8-1.pgdg110+1)
-- Dumped by pg_dump version 14.8 (Ubuntu 14.8-1.pgdg20.04+1)

-- Started on 2023-06-20 12:21:23 PDT

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
-- TOC entry 6 (class 2615 OID 16410)
-- Name: 123e4567-e89b-12d3-a456-426614174001; Type: SCHEMA; Schema: -; Owner: wds
--

CREATE SCHEMA "123e4567-e89b-12d3-a456-426614174001";


ALTER SCHEMA "123e4567-e89b-12d3-a456-426614174001" OWNER TO wds;

--
-- TOC entry 7 (class 2615 OID 16396)
-- Name: sys_wds; Type: SCHEMA; Schema: -; Owner: wds
--

CREATE SCHEMA sys_wds;


ALTER SCHEMA sys_wds OWNER TO wds;

--
-- TOC entry 842 (class 1247 OID 16404)
-- Name: array_of_file; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.array_of_file AS text[];


ALTER DOMAIN public.array_of_file OWNER TO wds;

--
-- TOC entry 836 (class 1247 OID 16400)
-- Name: array_of_relation; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.array_of_relation AS text[];


ALTER DOMAIN public.array_of_relation OWNER TO wds;

--
-- TOC entry 839 (class 1247 OID 16402)
-- Name: file; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.file AS text;


ALTER DOMAIN public.file OWNER TO wds;

--
-- TOC entry 833 (class 1247 OID 16398)
-- Name: relation; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.relation AS text;


ALTER DOMAIN public.relation OWNER TO wds;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 214 (class 1259 OID 16411)
-- Name: test; Type: TABLE; Schema: 123e4567-e89b-12d3-a456-426614174001; Owner: wds
--

CREATE TABLE "123e4567-e89b-12d3-a456-426614174001".test (
    sys_name text NOT NULL,
    "booleanAttr" boolean,
    "arrayBoolean" boolean[],
    "arrayNumber" numeric[],
    "stringAttr" text,
    "arrayString" text[],
    "numericAttr" numeric,
    "arrayDate" date[],
    "fileAttr" text,
    "arrayDateTime" timestamp with time zone[],
    "arrayFile" text[]
);


ALTER TABLE "123e4567-e89b-12d3-a456-426614174001".test OWNER TO wds;

--
-- TOC entry 212 (class 1259 OID 16391)
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
-- TOC entry 211 (class 1259 OID 16386)
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
-- TOC entry 213 (class 1259 OID 16405)
-- Name: instance; Type: TABLE; Schema: sys_wds; Owner: wds
--

CREATE TABLE sys_wds.instance (
    id uuid NOT NULL
);


ALTER TABLE sys_wds.instance OWNER TO wds;

--
-- TOC entry 3340 (class 0 OID 16411)
-- Dependencies: 214
-- Data for Name: test; Type: TABLE DATA; Schema: 123e4567-e89b-12d3-a456-426614174001; Owner: wds
--

COPY "123e4567-e89b-12d3-a456-426614174001".test (sys_name, "booleanAttr", "arrayBoolean", "arrayNumber", "stringAttr", "arrayString", "numericAttr", "arrayDate", "fileAttr", "arrayDateTime", "arrayFile") FROM stdin;
1	t	{t,f}	{12821.112,0.12121211,11}	string	{green,red}	123	{2022-11-03}	https://account_name.blob.core.windows.net/container-1/blob1	{"2022-11-03 11:36:20+00"}	{drs://drs.example.org/file_id_1,https://account_name.blob.core.windows.net/container-2/blob2}
\.


--
-- TOC entry 3338 (class 0 OID 16391)
-- Dependencies: 212
-- Data for Name: databasechangelog; Type: TABLE DATA; Schema: public; Owner: wds
--

COPY public.databasechangelog (id, author, filename, dateexecuted, orderexecuted, exectype, md5sum, description, comments, tag, liquibase, contexts, labels, deployment_id) FROM stdin;
20230426_syswds_schema	davidan	liquibase/changesets/20230426_syswds_schema.yaml	2023-06-20 11:56:41.068049	1	EXECUTED	8:b29510f962527373c017911411232c5a	sql		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_domains.yaml	2023-06-20 11:56:41.100602	2	EXECUTED	8:23c121fb77a7a2dcbf2761b7cf3851b3	sql		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_instance_table.yaml	2023-06-20 11:56:41.116706	3	EXECUTED	8:2d6affed0e524144ac539739229d7529	createTable tableName=instance		\N	4.21.1	\N	\N	7287400976
\.


--
-- TOC entry 3337 (class 0 OID 16386)
-- Dependencies: 211
-- Data for Name: databasechangeloglock; Type: TABLE DATA; Schema: public; Owner: wds
--

COPY public.databasechangeloglock (id, locked, lockgranted, lockedby) FROM stdin;
1	f	\N	\N
\.


--
-- TOC entry 3339 (class 0 OID 16405)
-- Dependencies: 213
-- Data for Name: instance; Type: TABLE DATA; Schema: sys_wds; Owner: wds
--

COPY sys_wds.instance (id) FROM stdin;
123e4567-e89b-12d3-a456-426614174001
\.


--
-- TOC entry 3197 (class 2606 OID 16417)
-- Name: test test_pkey; Type: CONSTRAINT; Schema: 123e4567-e89b-12d3-a456-426614174001; Owner: wds
--

ALTER TABLE ONLY "123e4567-e89b-12d3-a456-426614174001".test
    ADD CONSTRAINT test_pkey PRIMARY KEY (sys_name);


--
-- TOC entry 3193 (class 2606 OID 16390)
-- Name: databasechangeloglock databasechangeloglock_pkey; Type: CONSTRAINT; Schema: public; Owner: wds
--

ALTER TABLE ONLY public.databasechangeloglock
    ADD CONSTRAINT databasechangeloglock_pkey PRIMARY KEY (id);


--
-- TOC entry 3195 (class 2606 OID 16409)
-- Name: instance instance_pkey; Type: CONSTRAINT; Schema: sys_wds; Owner: wds
--

ALTER TABLE ONLY sys_wds.instance
    ADD CONSTRAINT instance_pkey PRIMARY KEY (id);


-- Completed on 2023-06-20 12:21:23 PDT

--
-- PostgreSQL database dump complete
--