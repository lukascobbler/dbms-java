package com.luka.simpledb.planningManagement;

// todo decide when to call folding for predicates
//  of SELECT, DELETE, UPDATE (AND CREATE VIEW since it uses SELECT)
//  (they aren't folded immediately in the
//  parser because they can contain non-constant values)
//  folding predicates is part of planning

// todo decide when to call folding for expressions of
//  UPDATE (they aren't folded immediately in the
//  parser because they can contain non-constant values)
//  folding expressions is part of planning

// todo decide when to call the running of expressions of
//  UPDATE, SELECT
//  running expressions is part of scanning

// todo decide when to call the running of predicates of
//  UPDATE, SELECT, DELETE
//  running predicates is part of scanning and is in the select scan

// project scan je uvek skroz gore, bez njega je samo ako select * i on zna samo za imena polja, ukljucujuci
// i imena virtuelnih polja

// extend scan se zove za svaki selection expression i on im daje default ime sem ako se ne iskoristi AS

// select scan samo filtrira

// JDBC ce imati vracanje seme pa cemo moci da znamo ime svega pa i extendovanih promenjivih, ali bi trebalo
// da ima i vracanje po broju argumenta

// za sve treba da se koristi getValue

//

public interface Plan {
    int distinctValues(String name);
}
