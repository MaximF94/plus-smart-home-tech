package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.enums.ConditionOperation;
import ru.yandex.practicum.analyzer.enums.ConditionType;
import ru.yandex.practicum.analyzer.model.Condition;

import java.util.Optional;

public interface ConditionRepository extends JpaRepository<Condition, Long> {
    Optional<Condition> findByTypeAndOperationAndValue(
            ConditionType type, ConditionOperation operation, Integer value);
}
