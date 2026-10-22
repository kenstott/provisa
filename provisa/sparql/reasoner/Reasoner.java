// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A sparql source's reasoner (REQ-2138): a separate program, run by its pinned Java runtime with
// HermiT and the OWL API on its class path (provisa/sparql/reasoner_bundle.py), never linked into
// Provisa. Two commands:
//
//   signature <axioms>                  the classes and properties the axioms name, one per line:
//                                       "class <iri>", "object <iri>" or "data <iri>"
//   classify <axioms> <slice.nt> <out>  the slice's facts about those classes and properties,
//                                       classified under the axioms; every membership of a
//                                       named class and every value of a named property that
//                                       follows is written to <out> as N-Triples
//
// The slice is N-Triples (a CONSTRUCT answer): a triple of rdf:type with one of the axioms'
// classes is a membership, a triple of one of their object properties with an IRI object is a
// link, of one of their data properties with a literal object a value; any other triple says
// nothing the axioms reason about and is not read. A failure exits non-zero with its reason on
// standard error.

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.semanticweb.HermiT.ReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDatatype;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.parameters.Imports;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

public final class Reasoner {
    private static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    private static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";
    private static final String RDF_LANG_STRING =
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("signature")) {
            signature(load(args[1]));
        } else if (args.length == 4 && args[0].equals("classify")) {
            classify(load(args[1]), Path.of(args[2]), Path.of(args[3]));
        } else {
            System.err.println(
                    "usage: Reasoner signature <axioms> | classify <axioms> <slice.nt> <out.nt>");
            System.exit(2);
        }
    }

    private static OWLOntology load(String axioms) throws Exception {
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        return manager.loadOntologyFromOntologyDocument(new File(axioms));
    }

    private static Set<OWLClass> classes(OWLOntology axioms) {
        Set<OWLClass> named = new TreeSet<>();
        axioms.classesInSignature(Imports.INCLUDED)
                .filter(c -> !c.isOWLThing() && !c.isOWLNothing())
                .forEach(named::add);
        return named;
    }

    private static Set<OWLObjectProperty> objectProperties(OWLOntology axioms) {
        Set<OWLObjectProperty> named = new TreeSet<>();
        axioms.objectPropertiesInSignature(Imports.INCLUDED)
                .filter(p -> !p.isOWLTopObjectProperty() && !p.isOWLBottomObjectProperty())
                .forEach(named::add);
        return named;
    }

    private static Set<OWLDataProperty> dataProperties(OWLOntology axioms) {
        Set<OWLDataProperty> named = new TreeSet<>();
        axioms.dataPropertiesInSignature(Imports.INCLUDED)
                .filter(p -> !p.isOWLTopDataProperty() && !p.isOWLBottomDataProperty())
                .forEach(named::add);
        return named;
    }

    private static void signature(OWLOntology axioms) {
        for (OWLClass c : classes(axioms)) {
            System.out.println("class " + c.getIRI());
        }
        for (OWLObjectProperty p : objectProperties(axioms)) {
            System.out.println("object " + p.getIRI());
        }
        for (OWLDataProperty p : dataProperties(axioms)) {
            System.out.println("data " + p.getIRI());
        }
    }

    private static void classify(OWLOntology axioms, Path slice, Path out) throws IOException {
        OWLOntologyManager manager = axioms.getOWLOntologyManager();
        OWLDataFactory df = manager.getOWLDataFactory();
        Set<IRI> classIris = new TreeSet<>();
        classes(axioms).forEach(c -> classIris.add(c.getIRI()));
        Set<IRI> objectIris = new TreeSet<>();
        objectProperties(axioms).forEach(p -> objectIris.add(p.getIRI()));
        Set<IRI> dataIris = new TreeSet<>();
        dataProperties(axioms).forEach(p -> dataIris.add(p.getIRI()));

        List<OWLAxiom> facts = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(slice, StandardCharsets.UTF_8)) {
            String line;
            int number = 0;
            while ((line = reader.readLine()) != null) {
                number++;
                Triple t = Triple.parse(line, number);
                if (t == null || !t.subject.isIri) {
                    continue;
                }
                OWLNamedIndividual s = df.getOWLNamedIndividual(IRI.create(t.subject.value));
                IRI predicate = IRI.create(t.predicate);
                if (t.predicate.equals(RDF_TYPE) && t.object.isIri) {
                    IRI cls = IRI.create(t.object.value);
                    if (classIris.contains(cls)) {
                        facts.add(df.getOWLClassAssertionAxiom(df.getOWLClass(cls), s));
                    }
                } else if (objectIris.contains(predicate) && t.object.isIri) {
                    facts.add(df.getOWLObjectPropertyAssertionAxiom(
                            df.getOWLObjectProperty(predicate),
                            s,
                            df.getOWLNamedIndividual(IRI.create(t.object.value))));
                } else if (dataIris.contains(predicate) && !t.object.isIri && !t.object.isBlank) {
                    facts.add(df.getOWLDataPropertyAssertionAxiom(
                            df.getOWLDataProperty(predicate), s, literal(df, t.object)));
                }
            }
        }
        manager.addAxioms(axioms, facts.stream());

        OWLReasoner reasoner = new ReasonerFactory().createReasoner(axioms);
        if (!reasoner.isConsistent()) {
            throw new IllegalStateException(
                    "the slice is inconsistent with the axioms: nothing follows from it");
        }
        reasoner.precomputeInferences(
                InferenceType.CLASS_ASSERTIONS, InferenceType.OBJECT_PROPERTY_ASSERTIONS);
        Set<OWLNamedIndividual> individuals = new TreeSet<>();
        axioms.individualsInSignature(Imports.INCLUDED).forEach(individuals::add);
        try (BufferedWriter writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            for (OWLClass c : classes(axioms)) {
                for (OWLNamedIndividual i :
                        reasoner.getInstances(c, false).entities().sorted().toList()) {
                    writer.write(iri(i.getIRI()) + " <" + RDF_TYPE + "> " + iri(c.getIRI()) + " .\n");
                }
            }
            for (OWLObjectProperty p : objectProperties(axioms)) {
                for (OWLNamedIndividual i : individuals) {
                    for (OWLNamedIndividual j :
                            reasoner.getObjectPropertyValues(i, p).entities().sorted().toList()) {
                        writer.write(iri(i.getIRI()) + " " + iri(p.getIRI()) + " "
                                + iri(j.getIRI()) + " .\n");
                    }
                }
            }
            for (OWLDataProperty p : dataProperties(axioms)) {
                for (OWLNamedIndividual i : individuals) {
                    for (OWLLiteral v : new TreeSet<>(reasoner.getDataPropertyValues(i, p))) {
                        writer.write(iri(i.getIRI()) + " " + iri(p.getIRI()) + " "
                                + literalText(v) + " .\n");
                    }
                }
            }
        }
        reasoner.dispose();
    }

    private static OWLLiteral literal(OWLDataFactory df, Term term) {
        if (term.language != null) {
            return df.getOWLLiteral(term.value, term.language);
        }
        OWLDatatype type = df.getOWLDatatype(IRI.create(term.datatype == null ? XSD_STRING : term.datatype));
        return df.getOWLLiteral(term.value, type);
    }

    private static String iri(IRI iri) {
        return "<" + iri + ">";
    }

    private static String literalText(OWLLiteral v) {
        String text = "\"" + escape(v.getLiteral()) + "\"";
        if (v.hasLang()) {
            return text + "@" + v.getLang();
        }
        String type = v.getDatatype().getIRI().toString();
        return type.equals(XSD_STRING) || type.equals(RDF_LANG_STRING) ? text : text + "^^<" + type + ">";
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    /** One term of an N-Triples line: an IRI, a blank node, or a literal. */
    private record Term(boolean isIri, boolean isBlank, String value, String datatype, String language) {}

    /** One N-Triples statement; {@code parse} returns null for a blank or comment line. */
    private record Triple(Term subject, String predicate, Term object) {
        static Triple parse(String line, int number) {
            Cursor c = new Cursor(line, number);
            c.skipSpace();
            if (c.done() || c.peek() == '#') {
                return null;
            }
            Term subject = c.term();
            c.skipSpace();
            Term predicate = c.term();
            if (!predicate.isIri) {
                throw c.error("a predicate is an IRI");
            }
            c.skipSpace();
            Term object = c.term();
            c.skipSpace();
            c.expect('.');
            return new Triple(subject, predicate.value, object);
        }
    }

    private static final class Cursor {
        private final String s;
        private final int number;
        private int i;

        Cursor(String s, int number) {
            this.s = s;
            this.number = number;
        }

        boolean done() {
            return i >= s.length();
        }

        char peek() {
            return s.charAt(i);
        }

        void skipSpace() {
            while (!done() && (peek() == ' ' || peek() == '\t')) {
                i++;
            }
        }

        void expect(char ch) {
            if (done() || peek() != ch) {
                throw error("expected '" + ch + "'");
            }
            i++;
        }

        IllegalArgumentException error(String what) {
            return new IllegalArgumentException("slice line " + number + ": " + what + ": " + s);
        }

        Term term() {
            if (done()) {
                throw error("a term is missing");
            }
            char ch = peek();
            if (ch == '<') {
                int end = s.indexOf('>', i);
                if (end < 0) {
                    throw error("an IRI is not closed");
                }
                String iri = unescape(s.substring(i + 1, end));
                i = end + 1;
                return new Term(true, false, iri, null, null);
            }
            if (ch == '_') {
                int start = i;
                while (!done() && peek() != ' ' && peek() != '\t') {
                    i++;
                }
                return new Term(false, true, s.substring(start, i), null, null);
            }
            if (ch == '"') {
                StringBuilder value = new StringBuilder();
                i++;
                while (true) {
                    if (done()) {
                        throw error("a literal is not closed");
                    }
                    char c = s.charAt(i++);
                    if (c == '"') {
                        break;
                    }
                    if (c == '\\') {
                        value.append(escaped());
                    } else {
                        value.append(c);
                    }
                }
                if (!done() && peek() == '@') {
                    int start = ++i;
                    while (!done() && (Character.isLetterOrDigit(peek()) || peek() == '-')) {
                        i++;
                    }
                    return new Term(false, false, value.toString(), null, s.substring(start, i));
                }
                if (s.startsWith("^^<", i)) {
                    i += 2;
                    Term type = term();
                    return new Term(false, false, value.toString(), type.value, null);
                }
                return new Term(false, false, value.toString(), null, null);
            }
            throw error("unexpected '" + ch + "'");
        }

        private String escaped() {
            if (done()) {
                throw error("an escape is cut off");
            }
            char c = s.charAt(i++);
            switch (c) {
                case 't': return "\t";
                case 'b': return "\b";
                case 'n': return "\n";
                case 'r': return "\r";
                case 'f': return "\f";
                case '"': return "\"";
                case '\'': return "'";
                case '\\': return "\\";
                case 'u': return codePoint(4);
                case 'U': return codePoint(8);
                default: throw error("unknown escape \\" + c);
            }
        }

        private String codePoint(int digits) {
            if (i + digits > s.length()) {
                throw error("an escape is cut off");
            }
            int cp = Integer.parseInt(s.substring(i, i + digits), 16);
            i += digits;
            return new String(Character.toChars(cp));
        }

        private String unescape(String iri) {
            if (iri.indexOf('\\') < 0) {
                return iri;
            }
            Cursor inner = new Cursor(iri, number);
            StringBuilder out = new StringBuilder();
            while (!inner.done()) {
                char c = inner.s.charAt(inner.i++);
                out.append(c == '\\' ? inner.escaped() : String.valueOf(c));
            }
            return out.toString();
        }
    }
}
