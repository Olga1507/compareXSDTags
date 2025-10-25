package ru.diasoft.comparexsdtags.service;

import lombok.extern.slf4j.Slf4j;
import ru.diasoft.comparexsdtags.model.ValidationResult;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.*;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
public class ValidationService {

    private static final String XS = "http://www.w3.org/2001/XMLSchema";
    private static final List<String> ENCODINGS = Arrays.asList(
            "UTF-8", "UTF-8-BOM", "windows-1251", "cp1251", "ISO-8859-1"
    );

    public ValidationResult validate(MultipartFile xsdFile, MultipartFile sqlFile) {
        try {
            Map<String, Integer> xsdPaths = parseXsd(xsdFile);
            Map<String, Integer> sqlPaths = extractSqlPaths(sqlFile);

            List<String> differences = new ArrayList<>();
            List<String> absences = new ArrayList<>();
            for (String path : xsdPaths.keySet()) {
                if (sqlPaths.containsKey(path)) {
                    int xsdReq = xsdPaths.get(path);
                    int sqlReq = sqlPaths.get(path);
                    if (xsdReq != sqlReq) {
                        differences.add(
                                "Расхождение для '" + path + "': " +
                                        "XSD=" + xsdReq + ", SQL=" + sqlReq
                        );
                    }
                }
                else {
                    absences.add("Отсутствует в sql-файле xsdPath '" + path);
                }
            }

            differences.addAll(absences);

            boolean valid = differences.isEmpty();
            return new ValidationResult(valid, differences);

        } catch (Exception e) {
            throw new RuntimeException("Ошибка валидации: " + e.getMessage(), e);
        }
    }

    private Map<String, Integer> parseXsd(MultipartFile file) throws Exception {
        String content = readFileWithEncoding(file);
        if (content == null) throw new IOException("Не удалось прочитать XSD");

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        // Важно! Делаем парсер чувствительным к namespace
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(content));
        Document doc = db.parse(is);

        XPathFactory xpf = XPathFactory.newInstance();
        XPath xpath = xpf.newXPath();

        // Используем local-name() для игнорирования всех префиксов и пространств имён
        // Находим корневой элемент <xs:schema>
        Node schemaNode = (Node) xpath.evaluate("/*[local-name()='schema']", doc, XPathConstants.NODE);
        if (schemaNode == null) {
            throw new Exception("Не найден корневой элемент schema");
        }

        // Получаем targetNamespace напрямую из атрибута
        String targetNs = ((Element) schemaNode).getAttribute("targetNamespace");
        if (targetNs == null || targetNs.isEmpty()) {
            // Попробуем найти по имени атрибута без префикса
            NamedNodeMap attrs = schemaNode.getAttributes();
            for (int i = 0; i < attrs.getLength(); i++) {
                Node attr = attrs.item(i);
                if ("targetNamespace".equals(attr.getNodeName())) {
                    targetNs = attr.getNodeValue();
                    break;
                }
            }
        }
        if (targetNs == null || targetNs.isEmpty()) {
            throw new Exception("Нет targetNamespace");
        }
        log.info("🔍 Определён targetNamespace: " + targetNs);

        // Находим элемент Document
        // Ищем среди дочерних элементов schema все xs:element с @name='Document'
        Node documentElem = (Node) xpath.evaluate(
                "/*[local-name()='schema']/*[local-name()='element'][@name='Document']",
                doc, XPathConstants.NODE);
        if (documentElem == null) {
            throw new Exception("❌ Не найден элемент <xs:element name='Document'/>");
        }

        // Получаем тип Document
        Element documentElement = (Element) documentElem;
        String docTypeName = documentElement.getAttribute("type");
        if (docTypeName == null || docTypeName.trim().isEmpty()) {
            throw new Exception("У Document нет атрибута type");
        }
        // Удаляем префикс, если есть
        if (docTypeName.contains(":")) {
            docTypeName = docTypeName.substring(docTypeName.indexOf(":") + 1);
        }
        log.info("🔍 Тип Document: " + docTypeName);

        // Находим определение типа Document (complexType)
        Node docTypeNode = (Node) xpath.evaluate(
                "/*[local-name()='schema']/*[local-name()='complexType'][@name='" + docTypeName + "']",
                doc, XPathConstants.NODE);
        if (docTypeNode == null) {
            throw new Exception("❌ Не найден тип Document: " + docTypeName);
        }

        // Ищем первый элемент в последовательности внутри complexType Document
        // Это будет наш корневой элемент сообщения (например, BkToCstmrDbtCdtNtfctn)
        Node firstMessageElement = (Node) xpath.evaluate(
                "./*[local-name()='sequence']/*[local-name()='element'][1]",
                docTypeNode, XPathConstants.NODE);
        if (firstMessageElement == null) {
            // Возможно, sequence находится глубже или используется choice
            firstMessageElement = (Node) xpath.evaluate(
                    ".//*[local-name()='sequence']/*[local-name()='element'][1]",
                    docTypeNode, XPathConstants.NODE);
        }
        if (firstMessageElement == null) {
            throw new Exception("❌ Внутри Document нет ни одного элемента в последовательности");
        }

        String rootElemName = ((Element) firstMessageElement).getAttribute("name");
        if (rootElemName == null || rootElemName.trim().isEmpty()) {
            throw new Exception("❌ Первый элемент внутри Document не имеет атрибута name");
        }
        log.info("📌 Корневой элемент сообщения: " + rootElemName);

        // Теперь находим тип этого корневого элемента
        String rootElementType = ((Element) firstMessageElement).getAttribute("type");
        if (rootElementType == null || rootElementType.trim().isEmpty()) {
            throw new Exception("❌ У корневого элемента '" + rootElemName + "' нет атрибута type");
        }
        if (rootElementType.contains(":")) {
            rootElementType = rootElementType.substring(rootElementType.indexOf(":") + 1);
        }

        // Находим определение типа корневого элемента
        Node rootTypeNode = (Node) xpath.evaluate(
                "/*[local-name()='schema']/*[local-name()='complexType'][@name='" + rootElementType + "']",
                doc, XPathConstants.NODE);
        if (rootTypeNode == null) {
            throw new Exception("❌ Не найден тип корневого элемента: " + rootElementType);
        }

        // Строим пути, начиная с /Document/{rootElemName}
        Map<String, Integer> paths = new LinkedHashMap<>();
        String rootPath = "/Document/" + rootElemName;
        boolean parentRequired = getMinOccurs(firstMessageElement) == 1;
        processComplexType((Element) rootTypeNode, rootPath, paths, xpath, parentRequired);

        return paths;
    }

    private int getMinOccurs(Node node) {
        if (node == null || !(node instanceof Element)) return 1;
        String val = ((Element) node).getAttribute("minOccurs");
        return "0".equals(val.trim()) ? 2 : 1;
    }

    private void processComplexType(Element ct, String currentPath, Map<String, Integer> paths,
                                    XPath xpath, boolean parentRequired) throws XPathExpressionException {

        // Обработка sequence
        NodeList sequenceList = (NodeList) xpath.evaluate("*[local-name()='sequence']", ct, XPathConstants.NODESET);
        for (int s = 0; s < sequenceList.getLength(); s++) {
            Element seq = (Element) sequenceList.item(s);
            NodeList elements = (NodeList) xpath.evaluate("*[local-name()='element']", seq, XPathConstants.NODESET);

            for (int j = 0; j < elements.getLength(); j++) {
                Element elem = (Element) elements.item(j);
                String name = elem.getAttribute("name");
                if (name.isEmpty()) continue;

                String typeName = elem.getAttribute("type");
                boolean isComplexTypeRef = !typeName.isEmpty() &&
                        hasComplexTypeDefinition(typeName, elem.getOwnerDocument(), xpath);

                // Если тип — complexType, НЕ добавляем сам элемент в paths
                // Только рекурсивно обрабатываем его содержимое
                if (isComplexTypeRef) {
                    boolean isRequiredHere = getMinOccurs(elem) == 1;
                    boolean effectiveRequired = parentRequired && isRequiredHere;
                    String newPath = currentPath + "/" + name;

                    // Рекурсия: раскрываем содержимое complexType
                    Node nestedType = (Node) xpath.evaluate(
                            "/*[local-name()='schema']/*[local-name()='complexType'][@name='" + extractLocalName(typeName) + "']",
                            elem.getOwnerDocument(), XPathConstants.NODE);
                    if (nestedType != null) {
                        processComplexType((Element) nestedType, newPath, paths, xpath, effectiveRequired);
                    }

                    // ⛔ Не добавляем сам путь /.../GrpHdr в paths!
                    continue;
                }

                // Если это простой тип (примитив), добавляем в paths
                String newPath = currentPath + "/" + name;
                boolean isRequiredHere = getMinOccurs(elem) == 1;
                boolean effectiveRequired = parentRequired && isRequiredHere;
                paths.put(newPath, effectiveRequired ? 1 : 2);

                // Обработка вложенного анонимного типа (анонимный complexType)
                NodeList anonChildren = elem.getChildNodes();
                for (int k = 0; k < anonChildren.getLength(); k++) {
                    Node child = anonChildren.item(k);
                    if ("complexType".equals(child.getLocalName())) {
                        processComplexType((Element) child, newPath, paths, xpath, effectiveRequired);
                        break;
                    }
                }
            }
        }

        // Обработка choice — все элементы необязательные
        NodeList choiceList = (NodeList) xpath.evaluate("*[local-name()='choice']", ct, XPathConstants.NODESET);
        for (int c = 0; c < choiceList.getLength(); c++) {
            Element choice = (Element) choiceList.item(c);
            NodeList elements = (NodeList) xpath.evaluate("*[local-name()='element']", choice, XPathConstants.NODESET);

            for (int j = 0; j < elements.getLength(); j++) {
                Element elem = (Element) elements.item(j);
                String name = elem.getAttribute("name");
                if (name.isEmpty()) continue;

                String typeName = elem.getAttribute("type");
                boolean isComplexTypeRef = !typeName.isEmpty() &&
                        hasComplexTypeDefinition(typeName, elem.getOwnerDocument(), xpath);

                String newPath = currentPath + "/" + name;

                if (isComplexTypeRef) {
                    // Раскрываем вложенный тип, но не добавляем сам choice-элемент
                    Node nestedType = (Node) xpath.evaluate(
                            "/*[local-name()='schema']/*[local-name()='complexType'][@name='" + extractLocalName(typeName) + "']",
                            elem.getOwnerDocument(), XPathConstants.NODE);
                    if (nestedType != null) {
                        processComplexType((Element) nestedType, newPath, paths, xpath, false);
                    }
                    continue;
                }

                // Простые поля внутри choice — всегда необязательные
                paths.put(newPath, 2);

                // Анонимный тип
                NodeList anonChildren = elem.getChildNodes();
                for (int k = 0; k < anonChildren.getLength(); k++) {
                    Node child = anonChildren.item(k);
                    if ("complexType".equals(child.getLocalName())) {
                        processComplexType((Element) child, newPath, paths, xpath, false);
                        break;
                    }
                }
            }
        }
    }

    private String extractLocalName(String typeName) {
        if (typeName.contains(":")) {
            return typeName.substring(typeName.indexOf(":") + 1);
        }
        return typeName;
    }

    private boolean hasComplexTypeDefinition(String typeName, Document doc, XPath xpath) {
        try {
            String localTypeName = extractLocalName(typeName);
            Node node = (Node) xpath.evaluate(
                    "/*[local-name()='schema']/*[local-name()='complexType'][@name='" + localTypeName + "']",
                    doc, XPathConstants.NODE);
            return node != null;
        } catch (Exception e) {
            return false;
        }
    }

    private Map<String, Integer> extractSqlPaths(MultipartFile file) {
        String content = readFileWithEncoding(file);
        if (content == null) return Map.of();

        //(/Document/[^']+)'[^,]*,\D*(\d+)[^,]*,\D*(\d+)
        Pattern pattern = Pattern.compile("(/Document/[^']+)'[^,]*,\\D*(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(content);

        Map<String, Integer> paths = new HashMap<>();
        while (matcher.find()) {
            String path = matcher.group(1);
            int required = Integer.parseInt(matcher.group(2));
            paths.put(path, required);
        }
        return paths;
    }

    private String readFileWithEncoding(MultipartFile file) {
        for (String enc : ENCODINGS) {
            try {
                String normalizedEnc = enc.equals("UTF-8-BOM") ? "UTF-8" : enc;
                byte[] bytes = file.getBytes();
                if (enc.equals("UTF-8-BOM") && bytes.length > 3 &&
                        bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                    return new String(bytes, 3, bytes.length - 3, StandardCharsets.UTF_8);
                }
                return new String(bytes, Charset.forName(normalizedEnc));
            } catch (Exception e) {
                continue;
            }
        }
        return null;
    }
}