import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { query, queryOne } from '../db';

const JWT_SECRET = process.env.JWT_SECRET || 'change-this-secret-in-production';
const JWT_EXPIRES_IN = '7d';

export interface User {
  id: number;
  email: string;
  username: string | null;
  created_at: Date;
}

export interface AuthResponse {
  user: User;
  token: string;
}

class AuthService {
  // Register new user
  async register(email: string, password: string, username?: string): Promise<AuthResponse> {
    // Check if email exists
    const existing = await queryOne<User>(
      'SELECT id FROM users WHERE email = $1',
      [email.toLowerCase()]
    );
    
    if (existing) {
      throw new Error('Email already registered');
    }

    // Check if username exists
    if (username) {
      const existingUsername = await queryOne<User>(
        'SELECT id FROM users WHERE username = $1',
        [username.toLowerCase()]
      );
      if (existingUsername) {
        throw new Error('Username already taken');
      }
    }

    // Hash password
    const passwordHash = await bcrypt.hash(password, 12);

    // Create user
    const [user] = await query<User>(
      `INSERT INTO users (email, password_hash, username) 
       VALUES ($1, $2, $3) 
       RETURNING id, email, username, created_at`,
      [email.toLowerCase(), passwordHash, username?.toLowerCase() || null]
    );

    // Generate token
    const token = this.generateToken(user.id);

    return { user, token };
  }

  // Login user
  async login(email: string, password: string): Promise<AuthResponse> {
    // Find user
    const user = await queryOne<User & { password_hash: string }>(
      'SELECT id, email, username, password_hash, created_at FROM users WHERE email = $1',
      [email.toLowerCase()]
    );

    if (!user) {
      throw new Error('Invalid email or password');
    }

    // Verify password
    const isValid = await bcrypt.compare(password, user.password_hash);
    if (!isValid) {
      throw new Error('Invalid email or password');
    }

    // Generate token
    const token = this.generateToken(user.id);

    // Remove password_hash from response
    const { password_hash, ...userWithoutPassword } = user;

    return { user: userWithoutPassword, token };
  }

  // Get user by ID
  async getUserById(id: number): Promise<User | null> {
    return queryOne<User>(
      'SELECT id, email, username, created_at FROM users WHERE id = $1',
      [id]
    );
  }

  // Generate JWT token
  generateToken(userId: number): string {
    return jwt.sign({ userId }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
  }

  // Verify JWT token
  verifyToken(token: string): { userId: number } | null {
    try {
      const decoded = jwt.verify(token, JWT_SECRET) as { userId: number };
      return decoded;
    } catch {
      return null;
    }
  }

  // Update password
  async updatePassword(userId: number, oldPassword: string, newPassword: string): Promise<boolean> {
    const user = await queryOne<{ password_hash: string }>(
      'SELECT password_hash FROM users WHERE id = $1',
      [userId]
    );

    if (!user) {
      throw new Error('User not found');
    }

    const isValid = await bcrypt.compare(oldPassword, user.password_hash);
    if (!isValid) {
      throw new Error('Invalid current password');
    }

    const newHash = await bcrypt.hash(newPassword, 12);
    await query(
      'UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2',
      [newHash, userId]
    );

    return true;
  }
}

export const authService = new AuthService();
