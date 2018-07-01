U = LOAD 'input/users.txt' AS (name: chararray, age: int);
UF = FILTER U BY age >= 18 AND age <= 25;

P = LOAD 'input/pages.txt' AS (user: chararray, url: chararray);

UFP = JOIN UF BY name, P BY user;
V = GROUP UFP BY url;
CV = FOREACH V GENERATE group, COUNT(UFP) as clicks;
O = ORDER CV BY clicks DESC;
TOP = LIMIT O 5;
STORE TOP INTO '$OUTPUT';
