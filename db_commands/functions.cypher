// сгенерировать граф для входной строки  
call apoc.custom.asProcedure(
    "generateGraph",
    'UNWIND range(0, size($input) - $k) AS X
    WITH left(right($input, size($input) - X), $k) AS kmervalue
    MERGE (g:Genome { id: $genomeId}) 
    MERGE (n:KMer { value: kmervalue real: 1}) 
    MERGE (n)-[r:BelongsTo]->(g)
    WITH n as suff
    OPTIONAL MATCH (pref:KMer) WHERE right(pref.value, $k-1)=left(suff.value, $k-1)
    MERGE (pref)-[r:Precedes { real: 1 }]->(suff)
    RETURN pref, suff;',
    "write",
    [["preff", "node"], ["suff", "node"]],
    [["input", "string"], ["genomeId", "int"], ["k", "int"]]
);

// сгенерировать связи потенциального следования реальный-мутация
call.apoc.custom.asProcedure(
    "generateRtoMPreceding",
    'MATCH (nInitial: KMer {real: 1})
    OPTIONAL MATCH (nInitial)-[m:MutatedInto]->(nMutated:KMer {mutated: 1})
    OPTIONAL MATCH (nInitial)-[p:Precedes {real: 1}]->(nNext: KMer {real: 1})
    OPTIONAL MATCH (nNext)-[nm:MutatedInto]->(nNextMutated: KMer {mutated: 1}) 
    WITH nInitial as nI, nNextMutated as nNM
    WHERE right(nI.value, $k-1) = left(nNM.value, $k-1)
    CALL apoc.merge.relationship(
        nI, 
        "Precedes",
        {},
        {potential: 1},
        nNM,
        {potential: 1}
    ) YIELD rel 
    return rel as potentialInitial;',
    "write",
    [["potentialInitial", "relationship"]],
    [["k", "int"]]
);

// сгенерировать связи потенциального следования мутация-реальный
call.apoc.custom.asProcedure(
    "generateMtoRPreceding",
    'MATCH (nInitial: KMer {real: 1})
    OPTIONAL MATCH (nInitial)-[m:MutatedInto]->(nMutated:KMer {mutated: 1})
    OPTIONAL MATCH (nInitial)-[p:Precedes {real: 1}]->(nNext: KMer {real: 1})
    WITH nMutated as nM, nNext as nN
    WHERE right(nM.value, $k-1) = left(nN.value, $k-1)
    CALL apoc.merge.relationship(
        nM, 
        "Precedes",
        {},
        {potential: 1},
        nN,
        {potential: 1}
    ) YIELD rel 
    return rel as potentialNext;',
    "write",
    [["potentialNext", "relationship"]],
    [["k", "int"]]
);

// сгенерировать связи потенциального следования мутация-мутация
call.apoc.custom.asProcedure(
    "generateMtoMPreceding",
    'MATCH (nInitial: KMer {real: 1})
    OPTIONAL MATCH (nInitial)-[m:MutatedInto]->(nMutated:KMer {mutated: 1})
    OPTIONAL MATCH (nInitial)-[p:Precedes {real: 1}]->(nNext: KMer {real: 1})
    OPTIONAL MATCH (nNext)-[nm:MutatedInto]->(nNextMutated: KMer {mutated: 1}) 
    WITH nMutated as nM, nNextMutated as nNM
    WHERE right(nM.value, $k-1) = left(nNM.value, $k-1)
    CALL apoc.merge.relationship(
        nM, 
        "Precedes",
        {},            
        {potential: 1}, 
        nNM,
        {potential: 1} 
    ) YIELD rel 
    return rel as potentialNextMutated;',
    "write",
    [["potentialNextMutated", "relationship"]],
    [["k", "int"]]
);

// вернуть комплементарное основание
CALL apoc.custom.asFunction(
    "complementBase",
    'RETURN CASE $input WHEN "A" THEN "C" WHEN "C" THEN "A" WHEN "G" THEN "T" WHEN "T" THEN "G" END',
    "string",
    [["input", "string"]]
);

// заменить основания по указанным индексам на комплементарные 
CALL apoc.custom.asFunction(
    "replaceBases",
    "return reduce(new_value=$input, indx IN $mutation_scheme | custom.replaceBaseByIndex(new_value, indx))",
    "string",
    [["input", "string"], ["mutation_scheme", "list of integer"]]
);

// заменить основание по указанному индексу на комплементарное 
CALL apoc.custom.asFunction(
    "replaceBaseByIndex",
    "return left($input, $index) + custom.complementBase(right(left($input, $index + 1), 1)) + right($input, size($input) - $index - 1)",
    "string",
    [["input", "string"], ["index", "integer"]]
);

// по переданному пути кмер восстановить строку 
CALL apoc.custom.asFunction(
    "pathToString",
    "with nodes($path) as nodes return reduce(str=head(nodes).value, n in tail(nodes) | str+right(n.value, 1));",
    "string",
    [["path", "path"]]
);

// сгенерировать мутации замены оснований для входного графа 
call apoc.custom.asProcedure(
    "generateReplacementsMutations",
    'WITH custom.replaceBases($n.value, $mutation_scheme) as mutatedValue
    CALL apoc.merge.node(
        ["KMer"],
        {value: mutatedValue},
        {mutated: 1},
        {mutated: 1}
    ) yield node 
    CALL apoc.merge.relationship(
        $n, "MutatedInto", {replacements:size($mutation_scheme)}, {},  node, {}
    ) yield rel 
    return node, rel;',
    "write",
    [["mutatedNode", "node"], ["mutationRel", "relationship"]],
    [["n", "node"], ["mutation_scheme", "list of int"]]
);

// сгенерировать связи потенциального следования 
call apoc.custom.asProcedure(
    "generateAllReplacementsMutations",
    'MATCH (n:KMer)
    CALL apoc.merge.relationship(n, "MutatedInto", {replacements:0}, {},  n, {}) yield rel  
    UNWIND apoc.coll.combinations(range(0, $k-1), 1, $k) as mutation_scheme  
    CALL custom.generateReplacementsMutations(n, mutation_scheme) YIELD mutatedNode
    return mutatedNode;',
    "write",
    [["mutatedNode", "node"]],
    [["k", "int"]]
);

// список нодов, которые могут мутировать в искомую ноду 
CALL apoc.custom.asFunction(
    "listSourcesOfMutation",
    "WITH $n as aliasedN
    MATCH (source:KMer)-[:MutatedInto]->(aliasedN) 
    RETURN source as sourcesList",
    "list",
    [["n", "node"]]
);