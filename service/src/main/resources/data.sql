--Add domains to distinguish relations and arrays of relations from strings and arrays of strings
DO
'
DECLARE
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.domains WHERE domain_name = ''relation'') THEN
        create domain relation as text;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.domains WHERE domain_name = ''array_of_relation'') THEN
         create domain array_of_relation as text[];
     END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.domains WHERE domain_name = ''file'') THEN
        create domain file as text;
    END IF;
END;
';