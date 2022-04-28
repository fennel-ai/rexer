package sagemaker

import (
	"database/sql"
	"fmt"
	"time"

	lib "fennel/lib/sagemaker"
	"fennel/tier"
)

func InsertModel(tier tier.Tier, model lib.Model) (uint32, error) {
	ts := time.Now().Unix()
	stmt := `
		INSERT IGNORE INTO model (
			name,
			version,
			framework,
			framework_version,
			artifact_path,
			last_modified
		) VALUES (
			?, ?, ?, ?, ?, ?
		)
	`
	res, err := tier.DB.Exec(stmt,
		model.Name,
		model.Version,
		model.Framework,
		model.FrameworkVersion,
		model.ArtifactPath,
		ts,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to create model entry in db: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert id: %v", err)
	}
	return uint32(id), nil
}

func MakeModelInactive(tier tier.Tier, name, version string) error {
	stmt := `
		UPDATE model
		SET active=false
		WHERE name=? AND version=?
	`
	_, err := tier.DB.Exec(stmt, name, version)
	if err != nil {
		return fmt.Errorf("failed to make model inactive: %v", err)
	}
	return nil
}

func GetModel(tier tier.Tier, id uint32) (lib.Model, error) {
	var model lib.Model
	err := tier.DB.Get(&model, `
		SELECT *
		FROM model
		WHERE id=?
	`, id)
	if err != nil {
		return model, fmt.Errorf("failed to get model: %v", err)
	}
	return model, nil
}

func GetActiveModels(tier tier.Tier) ([]lib.Model, error) {
	var models []lib.Model
	err := tier.DB.Select(&models, `
		SELECT *
		FROM model
		WHERE active=true
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get active models: %v", err)
	}
	return models, nil
}

func InsertHostedModels(tier tier.Tier, hostedModels ...lib.SagemakerHostedModel) error {
	stmt := `
		INSERT IGNORE INTO sagemaker_hosted_model (
			sagemaker_model_name,
			model_id,
			container_hostname
		) VALUES (
			:sagemaker_model_name,
			:model_id,
			:container_hostname
		)
	`
	_, err := tier.DB.NamedExec(stmt, hostedModels)
	if err != nil {
		return fmt.Errorf("failed to create hosted model entry in db: %v", err)
	}
	return nil
}

func GetHostedModels(tier tier.Tier, sagemakerModelName string) ([]lib.SagemakerHostedModel, error) {
	var hostedModels []lib.SagemakerHostedModel
	err := tier.DB.Select(&hostedModels, `
		SELECT *
		FROM sagemaker_hosted_model
		WHERE sagemaker_model_name=?
	`, sagemakerModelName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get hosted models: %v", err)
	}
	return hostedModels, nil
}

// GetCoveringHostedModels returns the hosted models that host only and all active models.
func GetCoveringHostedModels(tier tier.Tier) ([]string, error) {
	var hostedModelNames []string
	err := tier.DB.Select(&hostedModelNames, `
		SELECT name FROM
		(
			SELECT sagemaker_model_name as name, active_model_count FROM
			(
				SELECT sagemaker_hosted_model.sagemaker_model_name, COUNT(model.id) as active_model_count
				FROM sagemaker_hosted_model JOIN model
				ON sagemaker_hosted_model.model_id=model.id
				WHERE model.active=true
				GROUP BY sagemaker_hosted_model.sagemaker_model_name
			) covering_models
			WHERE	
				active_model_count = (
					SELECT COUNT(*)
					FROM model
					WHERE active=true
				)
		) covering_models_2
		WHERE
			active_model_count = (
			    SELECT COUNT(*)
			    FROM sagemaker_hosted_model
			    WHERE sagemaker_model_name=name
			)
	`)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get covering hosted model: %v", err)
	}
	return hostedModelNames, nil
}

func GetAllHostedModels(tier tier.Tier) ([]lib.SagemakerHostedModel, error) {
	var hostedModels []lib.SagemakerHostedModel
	err := tier.DB.Select(&hostedModels, `
		SELECT *
		FROM sagemaker_hosted_model
	`)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get hosted models: %v", err)
	}
	return hostedModels, nil
}

func InsertEndpointConfig(tier tier.Tier, cfg lib.SagemakerEndpointConfig) error {
	stmt := `
		INSERT INTO sagemaker_endpoint_config (
			name,
			variant_name,
			model_name,
			instance_type,
			instance_count
		) VALUES (
			?, ?, ?, ?, ?
		)
	`
	_, err := tier.DB.Exec(stmt,
		cfg.Name,
		cfg.VariantName,
		cfg.ModelName,
		cfg.InstanceType,
		cfg.InstanceCount,
	)
	if err != nil {
		return fmt.Errorf("failed to create endpoint config entry in db: %v", err)
	}
	return nil
}

func GetEndpointConfigWithModel(tier tier.Tier, sagemakerModelName string) (lib.SagemakerEndpointConfig, error) {
	var cfg lib.SagemakerEndpointConfig
	err := tier.DB.Get(&cfg, `
		SELECT *
		FROM sagemaker_endpoint_config
		WHERE model_name=?
	`, sagemakerModelName)
	if err == sql.ErrNoRows {
		return cfg, nil
	}
	if err != nil {
		return cfg, fmt.Errorf("failed to get endpoint config: %v", err)
	}
	return cfg, nil
}

func InsertEndpoint(tier tier.Tier, endpoint lib.SagemakerEndpoint) error {
	// Mark previous instances of this endpoint as not-active and then insert
	// the new endpoint into db. We do this in a txn to ensure that at least one
	// endpoint in the db is always marked as active.
	txn, err := tier.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to start txn: %v", err)
	}
	txn.Exec(`
		UPDATE sagemaker_endpoint SET active=false
		WHERE name=?
	`, endpoint.Name)
	txn.Exec(`
		INSERT INTO sagemaker_endpoint (
			name,
			endpoint_config_name
		) VALUES (
			?, ?
		)
	`, endpoint.Name, endpoint.EndpointConfigName)
	err = txn.Commit()
	if err != nil {
		return fmt.Errorf("failed to create endpoint entry in db: %v", err)
	}
	return nil
}

func GetEndpoint(tier tier.Tier, name string) (lib.SagemakerEndpoint, error) {
	var endpoint lib.SagemakerEndpoint
	err := tier.DB.Get(&endpoint, `
		SELECT *
		FROM sagemaker_endpoint
		WHERE name=? AND active=true
	`, name)
	if err == sql.ErrNoRows {
		return endpoint, nil
	}
	if err != nil {
		return endpoint, fmt.Errorf("failed to get endpoint: %v", err)
	}
	return endpoint, nil
}

func GetEndpointsWithCfg(tier tier.Tier, configName string) ([]lib.SagemakerEndpoint, error) {
	var endpoints []lib.SagemakerEndpoint
	err := tier.DB.Select(&endpoints, `
		SELECT *
		FROM sagemaker_endpoint
		WHERE endpoint_config_name=? AND active=true
	`, configName)
	if err != nil {
		return nil, fmt.Errorf("failed to get active endpoints: %v", err)
	}
	return endpoints, nil
}

func GetInactiveEndpoints(tier tier.Tier) ([]string, error) {
	var endpoints []string
	err := tier.DB.Select(&endpoints, `
		SELECT DISTINCT name
		FROM sagemaker_endpoint
		WHERE name NOT IN (
			SELECT name
			FROM sagemaker_endpoint
			WHERE active=true
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get inactive endpoints: %v", err)
	}
	return endpoints, nil
}

func MakeEndpointInactive(tier tier.Tier, endpointName string) error {
	_, err := tier.DB.Exec(`
		UPDATE sagemaker_endpoint
		SET active=false
		WHERE name=?
	`, endpointName)
	if err != nil {
		return fmt.Errorf("failed to make endpoint inactive: %v", err)
	}
	return nil
}
