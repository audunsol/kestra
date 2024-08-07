package io.kestra.plugin.core.dynamic;

import io.kestra.core.models.Property;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin
@Slf4j
public class DynamicPropertyExampleTask extends Task implements RunnableTask<VoidOutput> {
    @PluginProperty
    @NotNull
    private Property<Integer> number;

    @PluginProperty
    @NotNull
    private Property<String> string;

    @PluginProperty
    @Builder.Default
    private Property<String> withDefault = Property.of("Default Value");


    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        // No way to forget to render
        String str = string.as(runContext, String.class);
        // We support multiple types!
        Integer integer = number.as(runContext, Integer.class);

        log.info("{} - {}: {}", str, integer, withDefault.as(runContext, String.class));

        return null;
    }
}