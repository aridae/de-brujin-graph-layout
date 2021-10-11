// 1 - создаем граф для строки 
// CALL custom.generateGraph("ACCACCACCTG", 0, 3);

WITH 3 as k, "ACCACCACCTG" as input, 0 as genomeId
UNWIND range(0, size(input) - k) AS X
WITH left(right(input, size(input) - X), k) AS kmervalue, genomeId, k
MERGE (g:Genome { id: genomeId}) 
MERGE (n:KMer { value: kmervalue, real: 1}) 
MERGE (n)-[r:BelongsTo]->(g)
WITH n as suff, k
OPTIONAL MATCH (pref:KMer) WHERE right(pref.value, k-1)=left(suff.value, k-1)
MERGE (pref)-[r:Precedes { real: 1 }]->(suff)
RETURN pref, suff;


// 2 - генерируем все мутации 
// CALL custom.generateAllReplacementsMutations(3); 

WITH 3 as k 
MATCH (n:KMer)
CALL apoc.merge.relationship(n, "MutatedInto", {replacements:0}, {},  n, {}) yield rel  
UNWIND apoc.coll.combinations(range(0, k-1), 1, k) as mutation_scheme  
CALL custom.generateReplacementsMutations(n, mutation_scheme) YIELD mutatedNode
return mutatedNode;

// раскрашиваем красиво 
MATCH (n:KMer {real: 1})
SET n :Real
RETURN *;
MATCH (n:KMer {mutated: 1})
SET n :Mutated
RETURN *;

// 3 - генерируем все связи потенциального следования 
// CALL custom.generateRtoMPreceding(3);
// CALL custom.generateMtoRPreceding(3);
// CALL custom.generateMtoMPreceding(3);


MATCH (nInitial: KMer {real: 1})
OPTIONAL MATCH (nInitial)-[m:MutatedInto]->(nMutated:KMer {mutated: 1})
OPTIONAL MATCH (nInitial)-[p:Precedes {real: 1}]->(nNext: KMer {real: 1})
OPTIONAL MATCH (nNext)-[nm:MutatedInto]->(nNextMutated: KMer {mutated: 1}) 
WITH nInitial as nI, nNextMutated as nNM
WHERE right(nI.value, 2) = left(nNM.value, 2)
CALL apoc.merge.relationship(
    nI, 
    "Precedes",
    {},
    {potential: 1},
    nNM,
    {potential: 1}
) YIELD rel 
return rel as potentialInitial;

MATCH (nInitial: KMer {real: 1})
OPTIONAL MATCH (nInitial)-[m:MutatedInto]->(nMutated:KMer {mutated: 1})
OPTIONAL MATCH (nInitial)-[p:Precedes {real: 1}]->(nNext: KMer {real: 1})
WITH nMutated as nM, nNext as nN
WHERE right(nM.value, 2) = left(nN.value, 2)
CALL apoc.merge.relationship(
    nM, 
    "Precedes",
    {},
    {potential: 1},
    nN,
    {potential: 1}
) YIELD rel 
return rel as potentialNext;

MATCH (nInitial: KMer {real: 1})
OPTIONAL MATCH (nInitial)-[m:MutatedInto]->(nMutated:KMer {mutated: 1})
OPTIONAL MATCH (nInitial)-[p:Precedes {real: 1}]->(nNext: KMer {real: 1})
OPTIONAL MATCH (nNext)-[nm:MutatedInto]->(nNextMutated: KMer {mutated: 1}) 
WITH nMutated as nM, nNextMutated as nNM
WHERE right(nM.value, 2) = left(nNM.value, 2)
CALL apoc.merge.relationship(
    nM, 
    "Precedes",
    {},            
    {potential: 1}, 
    nNM,
    {potential: 1} 
) YIELD rel 
return rel as potentialNextMutated;

// 4 - запрос - можно ли получить искомую строку мутациями кмер, которые есть в бд
WITH 3 as k, "ACATG" as input
WITH  size(input) - k + 1 as kmerscnt, k, input 
UNWIND range(0, kmerscnt - 1) AS X
WITH left(right(input, kmerscnt + k - 1 - X), k) AS tmpkmer, kmerscnt
MATCH (selectedNode:KMer {value:tmpkmer})
WITH collect(selectedNode) as selectedNodes, kmerscnt
WITH selectedNodes, selectedNodes[0] as start, tail(selectedNodes) as tail, kmerscnt-1 as depth
CALL apoc.path.expandConfig( 
    start, 
    {
        whitelistNodes:selectedNodes, 
        minLevel:depth, 
        maxLevel:depth, 
        relationshipFilter:'Precedes>'
    }
) YIELD path 
WHERE all(index in range(0, size(selectedNodes)-1) WHERE selectedNodes[index] = nodes(path)[index])
RETURN path;


WITH 3 as k, "ACATT" as input
WITH  size(input) - k + 1 as kmerscnt, k, input 
UNWIND range(0, kmerscnt - 1) AS X
WITH left(right(input, kmerscnt + k - 1 - X), k) AS tmpkmer, kmerscnt
MATCH (selectedNode:KMer {value:tmpkmer})
WITH collect(selectedNode) as selectedNodes, kmerscnt
WITH selectedNodes, selectedNodes[0] as start, tail(selectedNodes) as tail, kmerscnt-1 as depth
CALL apoc.path.expandConfig( 
    start, 
    {
        whitelistNodes:selectedNodes, 
        minLevel:depth, 
        maxLevel:depth, 
        relationshipFilter:'Precedes>'
    }
) YIELD path 
WHERE all(index in range(0, size(selectedNodes)-1) WHERE selectedNodes[index] = nodes(path)[index])
WITH path, size(nodes(path))-1 as pathlen
CALL apoc.cypher.run(
    "MATCH projectedPath=()-[:Precedes*"+ pathlen +"]->() 
    WHERE all(i in range(0, "+ pathlen +") 
        WHERE nodes(projectedPath)[i] in custom.getListedSourcesOfMutation(nodes(actualPath)[i]))
    RETURN projectedPath;", 
    { actualPath: path }) 
YIELD value as pathMaps
return custom.pathToString(pathMaps.projectedPath);