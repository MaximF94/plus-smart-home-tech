package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.enums.ActionType;
import ru.yandex.practicum.analyzer.model.Action;

import java.util.Optional;

public interface ActionRepository extends JpaRepository<Action, Long> {
    Optional<Action> findByTypeAndValue(ActionType type, Integer value);
}
