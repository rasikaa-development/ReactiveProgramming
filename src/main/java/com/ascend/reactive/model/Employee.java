package com.ascend.reactive.model;

/**
 * Immutable employee data carrier shared by all exercises.
 *
 * <p>Being a {@code record}, every field is {@code final} and there are no setters, so an
 * {@code Employee} cannot be modified once created (relevant to Exercise 4's question
 * "Does doOnNext() modify an employee?").
 *
 * @param id         employee id
 * @param name       employee name
 * @param department department code (CSE, ECE, IT, ...)
 * @param salary     monthly salary in rupees
 */
public record Employee(int id, String name, String department, int salary) {
}
