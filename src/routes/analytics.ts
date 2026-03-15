import { Router, Request, Response } from 'express';
import { analyticsService } from '../services/analytics';

const router = Router();

// GET /analytics/open-interest/:symbol
router.get('/open-interest/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720); // Max 30 days
    
    const data = await analyticsService.getOpenInterestHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/funding-rate/:symbol
router.get('/funding-rate/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getFundingRateHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/volume/:symbol
router.get('/volume/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getVolumeHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/price/:symbol
router.get('/price/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getPriceHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/long-short-ratio/:symbol
router.get('/long-short-ratio/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getLongShortRatioHistory(symbol, hours);
    res.json({ symbol, hours, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/liquidation-heatmap/:symbol
router.get('/liquidation-heatmap/:symbol', async (req: Request, res: Response) => {
  try {
    const { symbol } = req.params;
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    const bucketSize = parseInt(req.query.bucketSize as string) || 100;
    
    const data = await analyticsService.getLiquidationHeatmap(symbol, bucketSize, hours);
    res.json({ symbol, hours, bucketSize, data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/correlation
router.get('/correlation', async (req: Request, res: Response) => {
  try {
    const hours = Math.min(parseInt(req.query.hours as string) || 168, 720);
    
    const data = await analyticsService.getCorrelationMatrix(hours);
    res.json({ hours, ...data });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

// GET /analytics/exchange-health
router.get('/exchange-health', async (req: Request, res: Response) => {
  try {
    const data = await analyticsService.getExchangeHealth();
    res.json(data);
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Failed to fetch data' });
  }
});

export default router;
