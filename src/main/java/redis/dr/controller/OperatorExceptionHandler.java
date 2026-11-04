package redis.dr.controller;

import org.springframework.context.annotation.Profile;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.validation.method.ParameterErrors;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import redis.dr.exception.ConflictException;
import redis.dr.exception.NotFoundException;
import redis.dr.exception.UnavailableException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Errors of the operator API as RFC 9457 problem details. Spring's own request errors (bad JSON, wrong types, missing
 * body) keep their 400 from {@link ResponseEntityExceptionHandler}; validation errors list each offending field.
 */
@RestControllerAdvice
@Profile("!tool")
public class OperatorExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ConflictException.class)
    ProblemDetail conflict(ConflictException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    ProblemDetail notFound(NotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(UnavailableException.class)
    ProblemDetail unavailable(UnavailableException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex, HttpHeaders headers,
                                                                  HttpStatusCode status, WebRequest request) {
        List<Map<String, String>> errors = new ArrayList<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            errors.add(Map.of("field", error.getField(), "message", String.valueOf(error.getDefaultMessage())));
        }
        return withErrors(ex.getBody(), errors, ex, headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleHandlerMethodValidationException(HandlerMethodValidationException ex,
                                                                           HttpHeaders headers, HttpStatusCode status,
                                                                           WebRequest request) {
        List<Map<String, String>> errors = new ArrayList<>();
        ex.getParameterValidationResults().forEach(result -> {
            String name = String.valueOf(result.getMethodParameter().getParameterName());
            if (result instanceof ParameterErrors body) {
                // a validated request body: report the field inside it
                for (FieldError error : body.getFieldErrors()) {
                    errors.add(Map.of("field", error.getField(), "message", String.valueOf(error.getDefaultMessage())));
                }
            } else {
                for (MessageSourceResolvable error : result.getResolvableErrors()) {
                    errors.add(Map.of("field", name, "message", String.valueOf(error.getDefaultMessage())));
                }
            }
        });
        return withErrors(ex.getBody(), errors, ex, headers, status, request);
    }

    private ResponseEntity<Object> withErrors(ProblemDetail problem, List<Map<String, String>> errors, Exception ex,
                                              HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        problem.setDetail("Invalid request");
        problem.setProperty("errors", errors);
        return handleExceptionInternal(ex, problem, headers, status, request);
    }
}
