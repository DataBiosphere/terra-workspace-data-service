--
-- PostgreSQL database dump
--

-- Dumped from database version 14.8 (Debian 14.8-1.pgdg110+1)
-- Dumped by pg_dump version 14.8 (Ubuntu 14.8-0ubuntu0.22.04.1)

-- Started on 2023-06-29 22:30:35 PDT

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
-- TOC entry 5 (class 2615 OID 16436)
-- Name: 123e4567-e89b-12d3-a456-426614174000; Type: SCHEMA; Schema: -; Owner: wds
--

CREATE SCHEMA "123e4567-e89b-12d3-a456-426614174000";


ALTER SCHEMA "123e4567-e89b-12d3-a456-426614174000" OWNER TO wds;

--
-- TOC entry 7 (class 2615 OID 16396)
-- Name: sys_wds; Type: SCHEMA; Schema: -; Owner: wds
--

CREATE SCHEMA sys_wds;


ALTER SCHEMA sys_wds OWNER TO wds;

--
-- TOC entry 844 (class 1247 OID 16404)
-- Name: array_of_file; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.array_of_file AS text[];


ALTER DOMAIN public.array_of_file OWNER TO wds;

--
-- TOC entry 838 (class 1247 OID 16400)
-- Name: array_of_relation; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.array_of_relation AS text[];


ALTER DOMAIN public.array_of_relation OWNER TO wds;

--
-- TOC entry 841 (class 1247 OID 16402)
-- Name: file; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.file AS text;


ALTER DOMAIN public.file OWNER TO wds;

--
-- TOC entry 835 (class 1247 OID 16398)
-- Name: relation; Type: DOMAIN; Schema: public; Owner: wds
--

CREATE DOMAIN public.relation AS text;


ALTER DOMAIN public.relation OWNER TO wds;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 216 (class 1259 OID 16437)
-- Name: test; Type: TABLE; Schema: 123e4567-e89b-12d3-a456-426614174000; Owner: wds
--

CREATE TABLE "123e4567-e89b-12d3-a456-426614174000".test (
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


ALTER TABLE "123e4567-e89b-12d3-a456-426614174000".test OWNER TO wds;

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
-- TOC entry 214 (class 1259 OID 16410)
-- Name: backup; Type: TABLE; Schema: sys_wds; Owner: wds
--

CREATE TABLE sys_wds.backup (
                                id uuid NOT NULL,
                                status character varying(10),
                                error character varying(2000),
                                createdtime timestamp without time zone,
                                completedtime timestamp without time zone,
                                filename character varying(255)
);


ALTER TABLE sys_wds.backup OWNER TO wds;

--
-- TOC entry 215 (class 1259 OID 16417)
-- Name: backup_requests; Type: TABLE; Schema: sys_wds; Owner: wds
--

CREATE TABLE sys_wds.backup_requests (
                                         sourceworkspaceid uuid,
                                         destinationworkspaceid uuid,
                                         status character varying(10)
);


ALTER TABLE sys_wds.backup_requests OWNER TO wds;

--
-- TOC entry 213 (class 1259 OID 16405)
-- Name: instance; Type: TABLE; Schema: sys_wds; Owner: wds
--

CREATE TABLE sys_wds.instance (
    id uuid NOT NULL
);


ALTER TABLE sys_wds.instance OWNER TO wds;

--
-- TOC entry 3352 (class 0 OID 16437)
-- Dependencies: 216
-- Data for Name: test; Type: TABLE DATA; Schema: 123e4567-e89b-12d3-a456-426614174000; Owner: wds
--

COPY "123e4567-e89b-12d3-a456-426614174000".test (sys_name, "booleanAttr", "arrayBoolean", "arrayNumber", "stringAttr", "arrayString", "numericAttr", "arrayDate", "fileAttr", "arrayDateTime", "arrayFile") FROM stdin;
1	t	{t,f}	{12821.112,0.12121211,11}	string	{green,red}	123	{2022-11-03}	https://account_name.blob.core.windows.net/container-1/blob1	{"2022-11-03 11:36:20+00"}	{drs://drs.example.org/file_id_1,https://account_name.blob.core.windows.net/container-2/blob2}
\.


--
-- TOC entry 3348 (class 0 OID 16391)
-- Dependencies: 212
-- Data for Name: databasechangelog; Type: TABLE DATA; Schema: public; Owner: wds
--

COPY public.databasechangelog (id, author, filename, dateexecuted, orderexecuted, exectype, md5sum, description, comments, tag, liquibase, contexts, labels, deployment_id) FROM stdin;
20230426_syswds_schema	davidan	liquibase/changesets/20230426_syswds_schema.yaml	2023-06-29 22:07:20.511648	1	EXECUTED	8:b29510f962527373c017911411232c5a	sql		\N	4.21.1	\N	\N	8101640417
20230426_syswds_schema	davidan	liquibase/changesets/20230426_domains.yaml	2023-06-29 22:07:20.531671	2	EXECUTED	8:23c121fb77a7a2dcbf2761b7cf3851b3	sql		\N	4.21.1	\N	\N	8101640417
20230426_syswds_schema	davidan	liquibase/changesets/20230426_instance_table.yaml	2023-06-29 22:07:20.546986	3	EXECUTED	8:2d6affed0e524144ac539739229d7529	createTable tableName=instance		\N	4.21.1	\N	\N	8101640417
20230612_backup_schema	yuliadub	liquibase/changesets/20230612_backup_table.yaml	2023-06-29 22:07:20.56315	4	EXECUTED	8:238700bb4ca8e35e78d210d2dfd60665	createTable tableName=backup		\N	4.21.1	\N	\N	8101640417
20230622_backup_requests_schema	yuliadub	liquibase/changesets/20230622_backup_requests_table.yaml	2023-06-29 22:07:20.575079	5	EXECUTED	8:0fe32e644a53dd3c9f9f937c24150806	createTable tableName=backup_requests		\N	4.21.1	\N	\N	8101640417
20230426_syswds_schema	davidan	liquibase/changesets/20230426_syswds_schema.yaml	2023-06-20 11:56:41.068049	1	EXECUTED	8:b29510f962527373c017911411232c5a	sql		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_domains.yaml	2023-06-20 11:56:41.100602	2	EXECUTED	8:23c121fb77a7a2dcbf2761b7cf3851b3	sql		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_instance_table.yaml	2023-06-20 11:56:41.116706	3	EXECUTED	8:2d6affed0e524144ac539739229d7529	createTable tableName=instance		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_syswds_schema.yaml	2023-06-20 11:56:41.068049	1	EXECUTED	8:b29510f962527373c017911411232c5a	sql		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_domains.yaml	2023-06-20 11:56:41.100602	2	EXECUTED	8:23c121fb77a7a2dcbf2761b7cf3851b3	sql		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_instance_table.yaml	2023-06-20 11:56:41.116706	3	EXECUTED	8:2d6affed0e524144ac539739229d7529	createTable tableName=instance		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_syswds_schema.yaml	2023-06-20 11:56:41.068049	1	EXECUTED	8:b29510f962527373c017911411232c5a	sql		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_domains.yaml	2023-06-20 11:56:41.100602	2	EXECUTED	8:23c121fb77a7a2dcbf2761b7cf3851b3	sql		\N	4.21.1	\N	\N	7287400976
20230426_syswds_schema	davidan	liquibase/changesets/20230426_instance_table.yaml	2023-06-20 11:56:41.116706	3	EXECUTED	8:2d6affed0e524144ac539739229d7529	createTable tableName=instance		\N	4.21.1	\N	\N	7287400976
\.


--
-- TOC entry 3347 (class 0 OID 16386)
-- Dependencies: 211
-- Data for Name: databasechangeloglock; Type: TABLE DATA; Schema: public; Owner: wds
--

COPY public.databasechangeloglock (id, locked, lockgranted, lockedby) FROM stdin;
1	f	\N	\N
\.


--
-- TOC entry 3350 (class 0 OID 16410)
-- Dependencies: 214
-- Data for Name: backup; Type: TABLE DATA; Schema: sys_wds; Owner: wds
--

COPY sys_wds.backup (id, status, error, createdtime, completedtime, filename) FROM stdin;
ab5b1a49-0543-4b7d-8d00-132a1df7a0bf	INITIATED	\N	2023-06-29 22:07:24.591	\N	\N
b3a88fb8-0a46-485f-88a8-b836b2bb926b	INITIATED	\N	2023-06-29 22:07:26.118	\N	\N
d82ca04f-6eee-40c4-9399-0f745d9d7b00	ERROR	\N	2023-06-29 22:08:42.334	\N	\N
c0bc149d-0aff-4a59-93fe-64b12e92e0a9	ERROR	\N	2023-06-29 22:08:43.719	\N	\N
629e2c78-1898-475e-a86d-0a8212e4dfb4	STARTED	\N	2023-06-29 22:18:02.881	\N	\N
5aeaeb06-9fdc-4910-8ecf-cd94fb36f737	ERROR	\N	2023-06-29 22:20:06.714	\N	\N
3cd09c1a-b60d-4864-ab18-ae6ef721bbb6	COMPLETED	\N	2023-06-29 22:20:08.188	\N	wdsservice/cloning/backup/123e4567-e89b-12d3-a456-426614174000-2023-06-29_22-20-08.sql
78278005-6802-4ad7-8bb0-8056d08fdbf0	ERROR	\N	2023-06-29 22:30:34.096	\N	\N
cd840a85-fef2-48fd-a990-e6fc88766446	STARTED	\N	2023-06-29 22:30:35.592	\N	\N
\.


--
-- TOC entry 3351 (class 0 OID 16417)
-- Dependencies: 215
-- Data for Name: backup_requests; Type: TABLE DATA; Schema: sys_wds; Owner: wds
--

COPY sys_wds.backup_requests (sourceworkspaceid, destinationworkspaceid, status) FROM stdin;
123e4567-e89b-12d3-a456-426614174001	123e4567-e89b-12d3-a456-426614174000	INITIATED
123e4567-e89b-12d3-a456-426614174001	123e4567-e89b-12d3-a456-426614174000	INITIATED
123e4567-e89b-12d3-a456-426614174001	123e4567-e89b-12d3-a456-426614174000	INITIATED
123e4567-e89b-12d3-a456-426614174001	123e4567-e89b-12d3-a456-426614174000	INITIATED
123e4567-e89b-12d3-a456-426614174000	123e4567-e89b-12d3-a456-426614174000	ERROR
123e4567-e89b-12d3-a456-426614174000	123e4567-e89b-12d3-a456-426614174000	ERROR
123e4567-e89b-12d3-a456-426614174000	123e4567-e89b-12d3-a456-426614174000	ERROR
123e4567-e89b-12d3-a456-426614174000	123e4567-e89b-12d3-a456-426614174000	ERROR
123e4567-e89b-12d3-a456-426614174001	123e4567-e89b-12d3-a456-426614174000	INITIATED
\.


--
-- TOC entry 3349 (class 0 OID 16405)
-- Dependencies: 213
-- Data for Name: instance; Type: TABLE DATA; Schema: sys_wds; Owner: wds
--

COPY sys_wds.instance (id) FROM stdin;
123e4567-e89b-12d3-a456-426614174000
\.


--
-- TOC entry 3207 (class 2606 OID 16443)
-- Name: test test_pkey; Type: CONSTRAINT; Schema: 123e4567-e89b-12d3-a456-426614174000; Owner: wds
--

ALTER TABLE ONLY "123e4567-e89b-12d3-a456-426614174000".test
    ADD CONSTRAINT test_pkey PRIMARY KEY (sys_name);


--
-- TOC entry 3201 (class 2606 OID 16390)
-- Name: databasechangeloglock databasechangeloglock_pkey; Type: CONSTRAINT; Schema: public; Owner: wds
--

ALTER TABLE ONLY public.databasechangeloglock
    ADD CONSTRAINT databasechangeloglock_pkey PRIMARY KEY (id);


--
-- TOC entry 3205 (class 2606 OID 16416)
-- Name: backup backup_pkey; Type: CONSTRAINT; Schema: sys_wds; Owner: wds
--

ALTER TABLE ONLY sys_wds.backup
    ADD CONSTRAINT backup_pkey PRIMARY KEY (id);


--
-- TOC entry 3203 (class 2606 OID 16409)
-- Name: instance instance_pkey; Type: CONSTRAINT; Schema: sys_wds; Owner: wds
--

ALTER TABLE ONLY sys_wds.instance
    ADD CONSTRAINT instance_pkey PRIMARY KEY (id);


-- Completed on 2023-06-29 22:30:35 PDT

--
-- PostgreSQL database dump complete
--
