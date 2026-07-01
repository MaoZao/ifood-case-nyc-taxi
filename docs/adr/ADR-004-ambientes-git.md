# ADR-004 — Estratégia de ambientes (Dev/Hom/Prd) via branches por ambiente

**Status:** Aceito · **Data:** 2026-06

## Contexto
A solução precisa de um caminho de promoção controlado entre **Dev**, **Hom**
(homologação) e **Prd** (produção), com isolamento de credenciais/dados e um
gate antes de produção.

## Decisão
Adotar **branches por ambiente** (`develop`→Dev, `hom`→Hom, `main`→Prd) com
**GitHub Environments**. Produção exige **aprovação manual** (*required
reviewers*). A configuração de cada ambiente é um overlay YAML
(`conf/pipeline.<env>.yaml`), selecionado automaticamente pela variável
`IFOOD_ENV` injetada pelo CI/CD.

## Alternativas consideradas
- **Trunk-based + tags/releases:** menos branches e muito usado em equipes
  maduras, mas exige forte disciplina de versionamento e oculta o "ambiente"
  atrás de tags — menos didático para demonstração.
- **GitFlow completo:** robusto, porém cerimonioso demais para o escopo.

## Consequências
- (+) Mapeamento branch→ambiente intuitivo, auditável e fácil de demonstrar.
- (+) Segredos isolados por ambiente (produção nunca exposta em dev/hom).
- (+) Gate de aprovação e *health check* pós-deploy aumentam a confiabilidade.
- (−) Back-merge de hotfix exige cuidado (documentado em `docs/cicd.md`).
- (→) Migração futura para trunk-based é possível sem mexer no código (a seleção
  por `IFOOD_ENV` é agnóstica de branch).

> Refs.: GitHub Docs — *Using environments for deployment*, *Managing
> environment protection rules*.
