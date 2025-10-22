package ru.diasoft.comparexsdtags.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.diasoft.comparexsdtags.model.ValidationResult;
import ru.diasoft.comparexsdtags.service.ValidationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;


@CrossOrigin(origins = "*", maxAge = 3600)
@Slf4j
@RestController
@RequestMapping("/api")
public class ValidationController {

    @Autowired
    private ValidationService validationService;

    @PostMapping("/validate")
    public ResponseEntity<ValidationResult> validate(
            @RequestParam("xsd") MultipartFile xsd,
            @RequestParam("sql") MultipartFile sql) {
        log.info("Начата обработка запроса");
        ValidationResult result = validationService.validate(xsd, sql);
        log.info("Обработка запроса завершена");
        return ResponseEntity.ok(result);
    }
}
