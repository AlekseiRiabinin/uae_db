#!/bin/bash

# setup_database.sh - Dubai Population Database Setup with logging

# Logging configuration
LOG_FILE="setup_database.log"
exec > >(tee -a "$LOG_FILE") 2>&1

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log_success() {
    log "✅ SUCCESS: $1"
}

log_info() {
    log "ℹ️  INFO: $1"
}

log_warning() {
    log "⚠️  WARNING: $1"
}

log_error() {
    log "❌ ERROR: $1"
}

log_step() {
    log "📋 STEP: $1"
}

# Header
echo "================================================"
log "Starting Dubai Population Database Setup"
echo "================================================"

# Step 1: Start the Docker containers
log_step "Starting Docker containers..."
docker compose up -d postgres

if [ $? -eq 0 ]; then
    log_success "Docker containers started"
else
    log_error "Failed to start Docker containers"
    exit 1
fi

# Wait for PostgreSQL to be ready
log_step "Waiting for PostgreSQL to be ready..."
MAX_RETRIES=30
RETRY_COUNT=0
POSTGRES_READY=false

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker compose exec postgres pg_isready -U postgres -d dubai_population >/dev/null 2>&1; then
        POSTGRES_READY=true
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    log_info "Waiting for PostgreSQL to be ready... (Attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

if [ "$POSTGRES_READY" = true ]; then
    log_success "PostgreSQL is running and accepting connections"
else
    log_error "PostgreSQL failed to start or is not ready after $MAX_RETRIES attempts"
    log_error "Check logs with: docker compose logs postgres"
    exit 1
fi

# Step 2: Create views and triggers with proper schema prefixes
log_step "Creating analytics views and triggers..."

# Create a temporary SQL file for better error handling
SQL_FILE=$(mktemp)
cat > "$SQL_FILE" << 'EOF'
-- Analytics views for reporting and visualization
CREATE OR REPLACE VIEW analytics.emirate_population AS
SELECT 
    pd.year,
    SUM(pd.population) as total_population,
    ROUND(SUM(pd.population) / NULLIF(SUM(c.area_km2), 0), 2) as density,
    COUNT(*) as communities_count,
    SUM(CASE WHEN pd.is_estimated THEN 1 ELSE 0 END) as estimated_years_count
FROM dubai.population_data pd
JOIN dubai.communities c ON pd.community_id = c.id
GROUP BY pd.year
ORDER BY pd.year;

CREATE OR REPLACE VIEW analytics.sector_population AS
SELECT 
    s.name_en as sector_name,
    pd.year,
    SUM(pd.population) as total_population,
    ROUND(SUM(pd.population) / NULLIF(SUM(c.area_km2), 0), 2) as density,
    COUNT(DISTINCT c.id) as communities_count
FROM dubai.population_data pd
JOIN dubai.communities c ON pd.community_id = c.id
JOIN dubai.sectors s ON c.sector_id = s.id
GROUP BY s.name_en, pd.year
ORDER BY s.name_en, pd.year;

CREATE OR REPLACE VIEW analytics.community_population AS
SELECT 
    c.code as community_code,
    c.name_en as community_name,
    s.name_en as sector_name,
    pd.year,
    pd.population,
    pd.density,
    pd.is_estimated,
    c.area_km2
FROM dubai.population_data pd
JOIN dubai.communities c ON pd.community_id = c.id
JOIN dubai.sectors s ON c.sector_id = s.id
ORDER BY c.name_en, pd.year;

-- Create update trigger for timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sectors_updated_at 
    BEFORE UPDATE ON dubai.sectors 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_communities_updated_at 
    BEFORE UPDATE ON dubai.communities 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_population_data_updated_at 
    BEFORE UPDATE ON dubai.population_data 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EOF

# Execute the SQL file
if docker compose exec -T postgres psql -U postgres -d dubai_population -f - < "$SQL_FILE"; then
    log_success "Views and triggers created successfully"
else
    log_error "Failed to create views and triggers"
    # Clean up temp file
    rm -f "$SQL_FILE"
    exit 1
fi

# Clean up temp file
rm -f "$SQL_FILE"

# Step 3: Start the remaining services
log_step "Starting API and data loader services..."
if docker compose up -d; then
    log_success "All services started successfully"
else
    log_error "Failed to start all services"
    exit 1
fi

# Final verification
log_step "Performing final verification..."
if docker compose ps | grep -q "running"; then
    log_success "All containers are running"
else
    log_warning "Some containers may not be running"
fi

# Success message
echo "================================================"
log_success "Dubai Population Database Setup Complete!"
echo "================================================"
log_info "API available at: http://localhost:8010"
log_info "Check logs: tail -f $LOG_FILE"
log_info "View database: docker compose exec postgres psql -U postgres -d dubai_population"
log_info "Test views: docker compose exec postgres psql -U postgres -d dubai_population -c 'SELECT * FROM analytics.emirate_population LIMIT 5;'"
echo "================================================"
